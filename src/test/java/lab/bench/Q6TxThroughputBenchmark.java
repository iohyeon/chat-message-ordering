package lab.bench;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import lab.Containers;
import lab.store.StoreFixture;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Q6 (2). producer 스레드 하나가 쉬지 않고 보낼 때 초당 커밋(또는 ack)된 레코드 수.
 *
 * <ul>
 *   <li>transactional-k: begin, 레코드 k개 비동기 send, commitTransaction()을 반복한다. 커밋이 끝난 레코드만 센다.</li>
 *   <li>idempotent-sync: 레코드마다 send().get()으로 ack를 기다린다. 트랜잭션 1건당 1레코드와 같은 "한 건씩 확정" 기준.</li>
 *   <li>idempotent-async: 비동기 send만 하고 ack 콜백을 센다. 멱등 producer가 낼 수 있는 상한.</li>
 * </ul>
 * 레코드 값은 100바이트, acks=all. 측정 후 read_committed 소비자로 실제 커밋된 레코드 수를 세어 producer 쪽 계수와 대조한다.
 */
@Testcontainers
@Tag("benchmark")
class Q6TxThroughputBenchmark {

    @Container
    static final KafkaContainer KAFKA = Containers.kafka();

    static final Duration WARMUP = Duration.ofSeconds(3);
    static final Duration MEASURE = Duration.ofSeconds(10);
    static final int REPEATS = 3;
    static final String SUMMARY = "q6_2_tx_throughput.csv";

    record Config(String mode, int recordsPerTx, int lingerMs) {
        String label() {
            return mode + (recordsPerTx > 0 ? "-" + recordsPerTx : "") + "/linger" + lingerMs;
        }
    }

    @Test
    void throughput() throws Exception {
        Bench.env("Q6-2");
        Bench.deleteIfExists(SUMMARY);
        Bench.deleteIfExists(FAILURES);
        // 클라이언트가 fatal 상태로 가는 이유를 남기도록 producer 내부 로그를 INFO로 올린다.
        ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("org.apache.kafka.clients.producer.internals"))
                .setLevel(ch.qos.logback.classic.Level.INFO);
        List<Config> configs = new ArrayList<>();
        for (int linger : new int[] {0, 5}) {
            configs.add(new Config("transactional", 1, linger));
            configs.add(new Config("transactional", 10, linger));
            configs.add(new Config("transactional", 100, linger));
            configs.add(new Config("idempotent-sync", 0, linger));
            configs.add(new Config("idempotent-async", 0, linger));
        }
        runOnce(new Config("transactional", 10, 0), 0);
        for (int rep = 1; rep <= REPEATS; rep++) {
            for (Config c : configs) {
                runOnce(c, rep);
            }
        }
    }

    static final String FAILURES = "q6_2_tx_failures.csv";

    /**
     * 한 설정을 돈다. 처음 실행에서 transactional-10의 커밋 루프가 InvalidTxnStateException으로 끝났으므로,
     * 실패하면 예외의 호출 위치와 브로커 로그를 남기고 같은 설정을 최대 3번까지 다시 돈다. 실패 횟수 자체도 결과다.
     */
    void runOnce(Config c, int rep) throws Exception {
        for (int attempt = 1; attempt <= 3; attempt++) {
            String txId = "tput-" + System.nanoTime();
            long t0 = System.nanoTime();
            try {
                runAttempt(c, rep, txId);
                return;
            } catch (Exception e) {
                double sec = (System.nanoTime() - t0) / 1e9;
                System.out.println("FAILURE " + c.label() + " rep=" + rep + " attempt=" + attempt + " txId=" + txId
                        + " after " + sec + "s: " + e);
                e.printStackTrace(System.out);
                String brokerLines = KAFKA.getLogs().lines()
                        .filter(l -> l.contains(txId) || l.contains("INVALID_TXN_STATE") || l.contains("InvalidTxnState"))
                        .reduce("", (a, b) -> a + "\n    " + b);
                System.out.println("BROKER-LOG" + brokerLines);
                Bench.append(FAILURES, "config,rep,attempt,seconds_into_run,exception",
                        String.format(Locale.ROOT, "%s,%d,%d,%.2f,%s", c.label(), rep, attempt, sec,
                                e.getClass().getSimpleName()));
            }
        }
        throw new IllegalStateException(c.label() + " rep " + rep + " 이 3번 모두 실패했다");
    }

    void runAttempt(Config c, int rep, String txId) throws Exception {
        String topic = "tput-" + c.label().replace('/', '-') + "-" + rep + "-" + System.nanoTime();
        StoreFixture.createTopic(KAFKA.getBootstrapServers(), topic);
        Map<String, Object> props = new HashMap<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.LINGER_MS_CONFIG, c.lingerMs()));
        boolean tx = c.mode().equals("transactional");
        if (tx) {
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
        }
        byte[] value = new byte[100];
        AtomicLong acked = new AtomicLong();
        long total = 0;
        long measuredRecords = 0;
        long measuredStart = 0, measuredEnd = 0;
        List<Long> commitNanos = new ArrayList<>();
        Bench.Gc gc0 = Bench.Gc.now();
        try (var producer = new KafkaProducer<byte[], byte[]>(props)) {
            if (tx) {
                producer.initTransactions();
            }
            long start = System.nanoTime();
            long warmEnd = start + WARMUP.toNanos();
            long end = warmEnd + MEASURE.toNanos();
            long countedAtWarmEnd = -1;
            while (true) {
                long now = System.nanoTime();
                if (countedAtWarmEnd < 0 && now >= warmEnd) {
                    countedAtWarmEnd = tx ? total : acked.get();
                    measuredStart = now;
                }
                if (now >= end) {
                    break;
                }
                switch (c.mode()) {
                    case "transactional" -> {
                        producer.beginTransaction();
                        for (int i = 0; i < c.recordsPerTx(); i++) {
                            producer.send(new ProducerRecord<>(topic, value));
                        }
                        long t0 = System.nanoTime();
                        producer.commitTransaction();
                        if (countedAtWarmEnd >= 0) {
                            commitNanos.add(System.nanoTime() - t0);
                        }
                        total += c.recordsPerTx();
                    }
                    case "idempotent-sync" -> {
                        producer.send(new ProducerRecord<>(topic, value)).get();
                        acked.incrementAndGet();
                        total++;
                    }
                    case "idempotent-async" -> {
                        producer.send(new ProducerRecord<>(topic, value), (md, e) -> {
                            if (e == null) {
                                acked.incrementAndGet();
                            }
                        });
                        total++;
                    }
                    default -> throw new IllegalArgumentException(c.mode());
                }
            }
            measuredEnd = System.nanoTime();
            measuredRecords = (tx ? total : acked.get()) - countedAtWarmEnd;
            producer.flush();
        }
        Bench.Gc gc = Bench.Gc.now().minus(gc0);
        double seconds = (measuredEnd - measuredStart) / 1e9;
        double perSec = measuredRecords / seconds;
        long committedInLog = countCommitted(topic);
        long[] cn = commitNanos.stream().mapToLong(Long::longValue).toArray();
        Bench.Summary cs = Bench.Summary.of(cn);
        String line = String.format(Locale.ROOT, "%s,%s,%d,%d,%d,%.1f,%d,%d,%d,%s,%d,%d",
                c.label(), c.mode(), c.recordsPerTx(), c.lingerMs(), rep, perSec, measuredRecords,
                tx ? total : acked.get(), committedInLog, cs.csv(), gc.count(), gc.millis());
        System.out.println("RESULT " + line);
        if (rep > 0) {
            Bench.append(SUMMARY, "config,mode,records_per_tx,linger_ms,rep,records_per_sec,measured_records,"
                    + "producer_total,read_committed_count,commit_call_" + Bench.Summary.HEADER.replace(",", ",commit_call_")
                    + ",gc_count,gc_ms", line);
        }
    }

    /** read_committed 소비자로 토픽 전체를 읽어 레코드 수를 센다. */
    static long countCommitted(String topic) {
        TopicPartition tp = new TopicPartition(topic, 0);
        try (var consumer = new KafkaConsumer<byte[], byte[]>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000))) {
            consumer.assign(Set.of(tp));
            consumer.seekToBeginning(Set.of(tp));
            long end = consumer.endOffsets(Set.of(tp)).get(tp);
            long n = 0;
            long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
            while (consumer.position(tp) < end && System.nanoTime() < deadline) {
                n += consumer.poll(Duration.ofMillis(200)).count();
            }
            return n;
        }
    }
}
