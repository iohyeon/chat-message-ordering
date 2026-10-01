package lab.bench;

import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import lab.Containers;
import lab.lease.LeaseRepository;
import lab.store.ChatRecord;
import lab.store.FenceMode;
import lab.store.MessageStore;
import lab.store.StoreFixture;
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
 * Q7 (2) 보충. 저장 워커가 밀려 있지 않을 때 교체 전에 쓴 레코드가 얼마나 거부되는지 로그 위에서 센다.
 *
 * <p>A가 1ms마다 레코드 하나를 비동기로 보내고(멱등 producer), 저장 워커는 같은 파티션을 계속 읽어 바로 저장한다.
 * A가 보내기 시작한 지 200ms에 B가 lease를 얻고(UPDATE 한 문장) 교체 표시를 로그에 쓴다. A는 모른 채 200ms 더 보낸다.
 * A의 레코드를 세 가지로 나눈다.
 * <ul>
 *   <li>P: B의 lease UPDATE를 시작하기 전에 브로커 ack를 받은 것. 교체 전에 정당하게 쓴 것이 분명하다.
 *   <li>M: P가 아니고 로그에서 표시보다 앞에 있는 것. lease 기준으로는 교체 뒤, 로그 기준으로는 교체 전이다.
 *   <li>Z: 로그에서 표시보다 뒤에 있는 것. 좀비 레코드다.
 * </ul>
 * 방식마다 새 토픽과 워커를 쓰고, 교체를 워밍업 3번 뒤 30번 반복한다.
 */
@Testcontainers
@Tag("benchmark")
class Q7PreTakeoverRejectBenchmark {

    @Container
    static final KafkaContainer KAFKA = Containers.kafka();

    static final int WARMUP = 3;
    static final int TAKEOVERS = 30;
    static final long INTERVAL_NANOS = 1_000_000L;
    static final Duration BEFORE = Duration.ofMillis(200);
    static final Duration AFTER = Duration.ofMillis(200);
    static final String FILE = "q7_2_pre_takeover_reject.csv";
    static final List<FenceMode> MODES = List.of(FenceMode.LEASE_EQ, FenceMode.LEASE_EQ_FOR_SHARE, FenceMode.LOG_ORDER,
            FenceMode.LOG_ORDER_FOR_SHARE);

    @Test
    void fsyncOff() throws Exception {
        try (PostgreSQLContainer pg = Containers.postgres()) {
            pg.start();
            run(pg, "fsync-off");
        }
    }

    @Test
    void fsyncOn() throws Exception {
        try (PostgreSQLContainer pg = Containers.postgres()) {
            pg.setCommand("postgres", "-c", "fsync=on");
            pg.start();
            run(pg, "fsync-on");
        }
    }

    void run(PostgreSQLContainer pg, String label) throws Exception {
        Bench.env("Q7-2 pre-takeover " + label);
        try (HikariDataSource ds = StoreFixture.dataSource(pg, 6);
             PrintWriter raw = Bench.gzipWriter("q7_2_pre_takeover_reject_raw_" + label + ".csv.gz",
                     "mode,takeover,seq,offset,class,inserted,ack_minus_update_start_us,stored_minus_update_end_us")) {
            StoreFixture.applySchema(ds);
            LeaseRepository leases = new LeaseRepository(ds);
            for (FenceMode mode : MODES) {
                String topic = "pre-" + mode + "-" + System.nanoTime();
                StoreFixture.createTopic(KAFKA.getBootstrapServers(), topic);
                try (var worker = new LiveWorker(topic, ds, mode);
                     var aProducer = producer("A-" + mode);
                     var bProducer = producer("B-" + mode)) {
                    worker.start();
                    for (int t = -WARMUP; t < TAKEOVERS; t++) {
                        takeover(ds, leases, worker, aProducer, bProducer, topic, mode, label, t, t >= 0 ? raw : null);
                    }
                }
            }
        }
    }

    static KafkaProducer<String, String> producer(String clientId) {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.LINGER_MS_CONFIG, 0,
                ProducerConfig.CLIENT_ID_CONFIG, clientId));
    }

    void takeover(HikariDataSource ds, LeaseRepository leases, LiveWorker worker, KafkaProducer<String, String> a,
            KafkaProducer<String, String> b, String topic, FenceMode mode, String label, int t, PrintWriter raw)
            throws Exception {
        String conv = "pre-" + mode + "-" + t;
        leases.create(conv, 4);
        leases.tryAcquire(conv, "A", Duration.ofSeconds(30)).orElseThrow();
        // A의 lease 기한이 지난 상태. A는 모르고 계속 쓴다. epoch는 아직 5다.
        leases.forceExpire(conv);
        if (mode.usesFence()) {
            try (Connection c = ds.getConnection(); var ps = c.prepareStatement(
                    "INSERT INTO message_fence (conversation_id, max_epoch) VALUES (?, 5)")) {
                ps.setString(1, conv);
                ps.executeUpdate();
            }
        }
        int n = (int) ((BEFORE.toNanos() + AFTER.toNanos()) / INTERVAL_NANOS);
        long[] ack = new long[n];
        long[] offset = new long[n];
        long start = System.nanoTime() + 20_000_000L;
        Thread sender = new Thread(() -> {
            for (int i = 0; i < n; i++) {
                long scheduled = start + i * INTERVAL_NANOS;
                long wait;
                while ((wait = scheduled - System.nanoTime()) > 0) {
                    LockSupport.parkNanos(Math.min(wait, 200_000));
                }
                final int idx = i;
                a.send(ChatRecord.message(conv, i + 1, 5, "A-" + (i + 1)).toProducerRecord(topic), (md, e) -> {
                    ack[idx] = System.nanoTime();
                    offset[idx] = e == null ? md.offset() : -1;
                });
            }
            a.flush();
        }, "sender-A");
        sender.start();
        long target = start + BEFORE.toNanos();
        long wait;
        while ((wait = target - System.nanoTime()) > 0) {
            LockSupport.parkNanos(Math.min(wait, 200_000));
        }
        long updStart = System.nanoTime();
        long epochB = leases.tryAcquire(conv, "B", Duration.ofSeconds(30)).orElseThrow();
        long updEnd = System.nanoTime();
        long markerOffset = b.send(ChatRecord.marker(conv, epochB).toProducerRecord(topic)).get().offset();
        long markerAcked = System.nanoTime();
        sender.join();
        long end = Math.max(markerOffset, maxOf(offset)) + 1;
        worker.awaitProcessed(end, Duration.ofSeconds(30));

        int nP = 0, pRej = 0, nM = 0, mRej = 0, nZ = 0, zStored = 0;
        for (int i = 0; i < n; i++) {
            long[] out = worker.outcomes.get(conv + "/" + (i + 1));
            if (out == null || offset[i] < 0) {
                throw new IllegalStateException("처리 결과 없음 " + conv + " seq=" + (i + 1));
            }
            boolean inserted = out[0] == 1;
            String cls;
            if (ack[i] < updStart) {
                cls = "P";
                nP++;
                pRej += inserted ? 0 : 1;
            } else if (offset[i] < markerOffset) {
                cls = "M";
                nM++;
                mRej += inserted ? 0 : 1;
            } else {
                cls = "Z";
                nZ++;
                zStored += inserted ? 1 : 0;
            }
            if (raw != null) {
                raw.printf(Locale.ROOT, "%s,%d,%d,%d,%s,%d,%d,%d%n", mode, t, i + 1, offset[i], cls, out[0],
                        (ack[i] - updStart) / 1000, (out[1] - updEnd) / 1000);
            }
        }
        String line = String.format(Locale.ROOT, "%s,%s,%d,%d,%d,%d,%d,%d,%d,%.3f,%.3f", label, mode, t, nP, pRej, nM,
                mRej, nZ, zStored, (updEnd - updStart) / 1e6, (markerAcked - updEnd) / 1e6);
        System.out.println((raw == null ? "WARMUP " : "RESULT ") + line);
        if (raw != null) {
            Bench.append(FILE, "postgres,mode,takeover,pre_n,pre_rejected,mid_n,mid_rejected,zombie_n,zombie_stored,"
                    + "lease_update_ms,marker_send_ms", line);
        }
    }

    static long maxOf(long[] a) {
        long m = -1;
        for (long v : a) {
            m = Math.max(m, v);
        }
        return m;
    }

    /** 파티션을 계속 읽어 레코드마다 바로 저장하는 워커. 결과를 (대화/seq) → {삽입 행 수, 저장 끝난 시각}으로 남긴다. */
    static final class LiveWorker extends Thread implements AutoCloseable {
        final String topic;
        final HikariDataSource ds;
        final FenceMode mode;
        final Map<String, long[]> outcomes = new ConcurrentHashMap<>();
        final AtomicLong processedUpTo = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean();
        volatile Throwable failure;

        LiveWorker(String topic, HikariDataSource ds, FenceMode mode) {
            super("live-worker-" + mode);
            this.topic = topic;
            this.ds = ds;
            this.mode = mode;
        }

        @Override
        public void run() {
            TopicPartition tp = new TopicPartition(topic, 0);
            try (var consumer = new KafkaConsumer<String, String>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                    ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1,
                    ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500));
                 Connection c = ds.getConnection()) {
                c.setAutoCommit(true);
                consumer.assign(Set.of(tp));
                consumer.seekToBeginning(Set.of(tp));
                while (!stop.get()) {
                    for (var cr : consumer.poll(Duration.ofMillis(50))) {
                        ChatRecord r = ChatRecord.from(cr);
                        if (r.marker()) {
                            if (mode.usesFence()) {
                                MessageStore.bumpFence(c, r.conversationId(), r.epoch());
                            }
                        } else {
                            int ins = MessageStore.insert(c, mode, r);
                            outcomes.put(r.conversationId() + "/" + r.seq(), new long[] {ins, System.nanoTime()});
                        }
                        processedUpTo.set(cr.offset() + 1);
                    }
                }
            } catch (Throwable e) {
                failure = e;
            }
        }

        void awaitProcessed(long endOffset, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (processedUpTo.get() < endOffset) {
                if (failure != null) {
                    throw new IllegalStateException("워커 실패", failure);
                }
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("워커가 " + endOffset + "까지 처리하지 못함: " + processedUpTo.get());
                }
                Thread.sleep(5);
            }
        }

        @Override
        public void close() throws InterruptedException {
            stop.set(true);
            join();
        }
    }
}
