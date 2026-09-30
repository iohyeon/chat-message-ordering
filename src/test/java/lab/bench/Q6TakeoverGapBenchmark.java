package lab.bench;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import lab.Containers;
import lab.lease.LeaseRepository;
import lab.store.ChatRecord;
import lab.store.FenceMode;
import lab.store.MessageStore;
import lab.store.StoreFixture;
import lab.store.StoreWorker;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Q6 (4). 담당 교체 공백: 새 담당자가 lease 획득을 시작해서 첫 레코드를 확정할 때까지. 단계별 시각을 남긴다.
 *
 * <p>broker 경로(브로커 fencing): lease UPDATE, 트랜잭션 producer 생성, initTransactions(), read_committed replay(로그 끝 부근을 읽어
 * 마지막 seq 확인), 첫 트랜잭션 커밋. 옛 담당자가 트랜잭션을 열어 둔 채 멈춘 경우(open)와 깨끗이 닫은 경우(clean)를 나눈다.
 * <p>store 경로(저장소 fencing, LEASE_EQ): lease UPDATE, 멱등 producer 생성, 저장소에서 마지막 seq 읽기, 첫 레코드 브로커 ack.
 * <p>store-marker 경로(LOG_ORDER): lease UPDATE, 멱등 producer 생성, 표시 레코드 전송, 저장 워커가 표시를 처리할 때까지 대기,
 * 저장소에서 마지막 seq 읽기, 첫 레코드 브로커 ack. 저장 워커는 별도 스레드에서 계속 돈다.
 */
@Testcontainers
@Tag("benchmark")
class Q6TakeoverGapBenchmark {

    @Container
    static final KafkaContainer KAFKA = Containers.kafka();

    @Container
    static final PostgreSQLContainer POSTGRES = Containers.postgres();

    static final int WARMUP = 5;
    static final int ITERATIONS = 30;
    static final String SUMMARY = "q6_4_takeover_gap_summary.csv";
    static final String RAW = "q6_4_takeover_gap_raw.csv";

    static HikariDataSource ds;
    static LeaseRepository leases;

    @Test
    void takeoverGap() throws Exception {
        Bench.env("Q6-4");
        Bench.deleteIfExists(SUMMARY);
        Bench.deleteIfExists(RAW);
        ds = StoreFixture.dataSource(POSTGRES, 4);
        try {
            StoreFixture.applySchema(ds);
            leases = new LeaseRepository(ds);
            record Variant(String path, String old, int retryBackoffMs, boolean spin) {
                Variant(String path, String old, int retryBackoffMs) {
                    this(path, old, retryBackoffMs, false);
                }
            }
            List<Variant> variants = List.of(
                    new Variant("broker", "clean", 100),
                    new Variant("broker", "clean", 10),
                    // 위 두 변형은 단계마다 속도가 달랐다(DB lease 획득까지). 긴 대기 동안 CPU가 쉬는 영향인지 보려고
                    // 한 코어를 계속 돌리는 스레드를 두고 다시 잰다.
                    new Variant("broker", "clean", 100, true),
                    new Variant("broker", "open", 100),
                    new Variant("broker", "open", 10),
                    new Variant("store", "open", 100),
                    new Variant("store-marker", "open", 100));
            for (Variant v : variants) {
                String label = v.path() + "/old-" + v.old() + "/retry.backoff" + v.retryBackoffMs() + (v.spin() ? "/spin" : "");
                AtomicBoolean spinStop = new AtomicBoolean();
                Thread spinner = null;
                if (v.spin()) {
                    spinner = new Thread(() -> {
                        while (!spinStop.get()) {
                            Thread.onSpinWait();
                        }
                    }, "spinner");
                    spinner.start();
                }
                Map<String, List<Long>> phases = new java.util.LinkedHashMap<>();
                switch (v.path()) {
                    case "broker" -> broker(label, v.old().equals("open"), v.retryBackoffMs(), phases);
                    case "store" -> store(label, false, phases);
                    case "store-marker" -> store(label, true, phases);
                    default -> throw new IllegalArgumentException();
                }
                spinStop.set(true);
                if (spinner != null) {
                    spinner.join();
                }
                for (var e : phases.entrySet()) {
                    long[] a = e.getValue().stream().mapToLong(Long::longValue).toArray();
                    Bench.Summary s = Bench.Summary.of(a);
                    String line = label + "," + e.getKey() + "," + s.csv();
                    System.out.println("RESULT " + line);
                    Bench.append(SUMMARY, "variant,phase," + Bench.Summary.HEADER, line);
                }
            }
        } finally {
            ds.close();
        }
    }

    static Map<String, Object> producerProps(String txId, int retryBackoffMs) {
        Map<String, Object> p = new HashMap<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.LINGER_MS_CONFIG, 0,
                ProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs));
        if (txId != null) {
            p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
        }
        return p;
    }

    static void record(Map<String, List<Long>> phases, String label, int iter, String phase, long nanos) {
        phases.computeIfAbsent(phase, k -> new ArrayList<>()).add(nanos);
        Bench.append(RAW, "variant,iteration,phase,ms", String.format(Locale.ROOT, "%s,%d,%s,%.3f", label, iter, phase, nanos / 1e6));
    }

    void broker(String label, boolean oldOpen, int retryBackoffMs, Map<String, List<Long>> phases) throws Exception {
        String topic = "gap-" + System.nanoTime();
        String txId = "shard-" + System.nanoTime();
        String conv = "gap-" + System.nanoTime();
        StoreFixture.createTopic(KAFKA.getBootstrapServers(), topic);
        leases.create(conv, 0);
        long seq = 0;
        // 로그에 커밋된 레코드 1,000건을 먼저 둔다.
        long epoch0 = leases.tryAcquire(conv, "owner-0", Duration.ofSeconds(30)).orElseThrow();
        var current = new KafkaProducer<String, String>(producerProps(txId, retryBackoffMs));
        current.initTransactions();
        current.beginTransaction();
        for (int i = 0; i < 1000; i++) {
            current.send(ChatRecord.message(conv, ++seq, epoch0, "m").toProducerRecord(topic));
        }
        current.commitTransaction();

        for (int iter = -WARMUP; iter < ITERATIONS; iter++) {
            // 옛 담당자 상태를 만든다.
            if (oldOpen) {
                current.beginTransaction();
                current.send(ChatRecord.message(conv, seq + 1, 0, "zombie").toProducerRecord(topic)).get();
                current.send(ChatRecord.message(conv, seq + 2, 0, "zombie").toProducerRecord(topic)).get();
                // 커밋하지 않고 멈춘다.
            } else {
                current.close();
            }
            leases.forceExpire(conv);

            long t0 = System.nanoTime();
            long epoch = leases.tryAcquire(conv, "owner-" + iter, Duration.ofSeconds(30)).orElseThrow();
            long t1 = System.nanoTime();
            var next = new KafkaProducer<String, String>(producerProps(txId, retryBackoffMs));
            long t2 = System.nanoTime();
            next.initTransactions();
            long t3 = System.nanoTime();
            Replay replay = replayLastSeq(topic);
            long last = replay.lastSeq();
            long t4 = System.nanoTime();
            next.beginTransaction();
            next.send(ChatRecord.message(conv, last + 1, epoch, "first").toProducerRecord(topic));
            next.commitTransaction();
            long t5 = System.nanoTime();
            // 소비자 close는 수락과 무관하다. 기본 close는 진행 중인 fetch(최대 fetch.max.wait.ms)를 기다리므로 따로 잰다.
            replay.consumer().close();
            long t6 = System.nanoTime();
            if (iter < 3) {
                System.out.printf(Locale.ROOT, "REPLAY %s | close %.1fms%n", replay.trace(), (t6 - t5) / 1e6);
            }
            if (last != seq) {
                throw new IllegalStateException("replay가 본 마지막 seq " + last + " != " + seq);
            }
            seq = last + 1;
            if (oldOpen) {
                current.close(Duration.ZERO);
            }
            current = next;
            if (iter >= 0) {
                record(phases, label, iter, "lease_acquire", t1 - t0);
                record(phases, label, iter, "producer_new", t2 - t1);
                record(phases, label, iter, "initTransactions", t3 - t2);
                record(phases, label, iter, "replay", t4 - t3);
                record(phases, label, iter, "first_commit", t5 - t4);
                record(phases, label, iter, "total_until_ready", t4 - t0);
                record(phases, label, iter, "total_until_first_record", t5 - t0);
                record(phases, label, iter, "total_without_initTransactions", (t4 - t0) - (t3 - t2));
                record(phases, label, iter, "replay_consumer_close_after", t6 - t5);
            }
        }
        current.close();
    }

    /** replay 결과. 소비자 close는 수락 재개와 무관하므로 끝난 뒤 따로 닫고 그 시간을 따로 잰다. */
    record Replay(long lastSeq, KafkaConsumer<String, String> consumer, String trace) {
    }

    /** read_committed로 로그 끝 부근을 읽어 마지막 seq를 찾는다. 새 담당자가 만드는 소비자 생성도 포함한다. */
    static Replay replayLastSeq(String topic) {
        TopicPartition tp = new TopicPartition(topic, 0);
        long c0 = System.nanoTime();
        var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"));
        consumer.assign(Set.of(tp));
        // read_committed 소비자의 endOffsets는 LSO다.
        long t0 = System.nanoTime();
        long lso = consumer.endOffsets(Set.of(tp)).get(tp);
        StringBuilder trace = new StringBuilder(String.format(Locale.ROOT, "create=%.1fms endOffsets=%.1fms lso=%d",
                (t0 - c0) / 1e6, (System.nanoTime() - t0) / 1e6, lso));
        consumer.seek(tp, Math.max(0, lso - 50));
        long last = -1;
        while (consumer.position(tp) < lso) {
            long a = System.nanoTime();
            int n = 0;
            for (var r : consumer.poll(Duration.ofMillis(100))) {
                last = Math.max(last, ChatRecord.from(r).seq());
                n++;
            }
            trace.append(String.format(Locale.ROOT, " | poll %.1fms n=%d pos=%d", (System.nanoTime() - a) / 1e6, n,
                    consumer.position(tp)));
        }
        return new Replay(last, consumer, trace.toString());
    }

    void store(String label, boolean marker, Map<String, List<Long>> phases) throws Exception {
        String topic = "gap-" + System.nanoTime();
        String conv = "gap-" + System.nanoTime();
        StoreFixture.createTopic(KAFKA.getBootstrapServers(), topic);
        leases.create(conv, 0);
        FenceMode mode = marker ? FenceMode.LOG_ORDER : FenceMode.LEASE_EQ;
        long epoch0 = leases.tryAcquire(conv, "owner-0", Duration.ofSeconds(30)).orElseThrow();
        long seq = 0;
        try (Connection c = ds.getConnection()) {
            MessageStore.bumpFence(c, conv, epoch0);
            for (int i = 0; i < 1000; i++) {
                MessageStore.insert(c, FenceMode.PLAIN, ChatRecord.message(conv, ++seq, epoch0, "m"));
            }
        }
        // 저장 워커를 계속 돌린다(표시 처리 대기에 필요).
        AtomicBoolean stop = new AtomicBoolean();
        Thread worker = null;
        if (marker) {
            worker = new Thread(() -> {
                try (var w = new StoreWorker(KAFKA.getBootstrapServers(), topic, ds, mode)) {
                    while (!stop.get()) {
                        w.drainToEnd(Duration.ofSeconds(30));
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    if (!stop.get()) {
                        e.printStackTrace();
                    }
                }
            }, "store-worker");
            worker.start();
        }
        var current = new KafkaProducer<String, String>(producerProps(null, 100));
        current.send(ChatRecord.message(conv, seq, epoch0, "warm").toProducerRecord(topic)).get();
        try (Connection c = ds.getConnection()) {
            for (int iter = -WARMUP; iter < ITERATIONS; iter++) {
                leases.forceExpire(conv);
                long t0 = System.nanoTime();
                long epoch = leases.tryAcquire(conv, "owner-" + iter, Duration.ofSeconds(30)).orElseThrow();
                long t1 = System.nanoTime();
                var next = new KafkaProducer<String, String>(producerProps(null, 100));
                long t2 = System.nanoTime();
                long tMarker = t2;
                if (marker) {
                    next.send(ChatRecord.marker(conv, epoch).toProducerRecord(topic)).get();
                    tMarker = System.nanoTime();
                    // 저장 워커가 표시를 처리해 fence가 새 epoch가 될 때까지 기다린다.
                    while (fence(c, conv) < epoch) {
                        Thread.onSpinWait();
                    }
                }
                long t3 = System.nanoTime();
                long last = MessageStore.maxSeq(c, conv);
                long t4 = System.nanoTime();
                ChatRecord first = ChatRecord.message(conv, last + 1, epoch, "first");
                next.send(first.toProducerRecord(topic)).get();
                long t5 = System.nanoTime();
                // 다음 반복의 기준을 맞추려고 첫 레코드를 저장소에도 넣는다(워커가 없는 store 경로).
                if (!marker) {
                    MessageStore.insert(c, mode, first);
                } else {
                    while (MessageStore.maxSeq(c, conv) < last + 1) {
                        Thread.onSpinWait();
                    }
                }
                current.close(Duration.ZERO);
                current = next;
                if (iter >= 0) {
                    record(phases, label, iter, "lease_acquire", t1 - t0);
                    record(phases, label, iter, "producer_new", t2 - t1);
                    if (marker) {
                        record(phases, label, iter, "marker_send", tMarker - t2);
                        record(phases, label, iter, "marker_stored_wait", t3 - tMarker);
                    }
                    record(phases, label, iter, "replay_db_max", t4 - t3);
                    record(phases, label, iter, "first_send_ack", t5 - t4);
                    record(phases, label, iter, "total_until_ready", t4 - t0);
                    record(phases, label, iter, "total_until_first_record", t5 - t0);
                }
            }
        } finally {
            current.close();
            stop.set(true);
            if (worker != null) {
                worker.join(10_000);
            }
        }
    }

    static long fence(Connection c, String conv) throws Exception {
        try (var ps = c.prepareStatement("SELECT max_epoch FROM message_fence WHERE conversation_id = ?")) {
            ps.setString(1, conv);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : -1;
            }
        }
    }
}
