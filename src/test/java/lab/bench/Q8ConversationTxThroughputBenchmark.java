package lab.bench;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;
import lab.Containers;
import lab.store.StoreFixture;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Q8 (3). 대화 하나(파티션 하나, 키 하나, transactional.id 하나)를 트랜잭션 producer 하나가 쓰고 10ms마다 커밋할 때,
 * 받는 부하에 따라 초당 커밋되는 레코드 수와 트랜잭션당 레코드 수가 어떻게 되는가.
 *
 * <p>Q7 (3)의 브로커 쪽 루프와 같다. 레코드마다 정해진 시각에 비동기 send() 하고, 마지막 커밋에서 10ms가 지나면
 * commitTransaction() 하고 새 트랜잭션을 연다. 부하 0은 쉬지 않고 보내는 경우다. 수락 지연은 예정 시각부터 그 레코드가 든
 * 트랜잭션의 commitTransaction() 이 돌아온 시각까지다. 레코드는 100바이트, acks=all, linger.ms=0.
 */
@Testcontainers
@Tag("benchmark")
class Q8ConversationTxThroughputBenchmark {

    @Container
    static final KafkaContainer KAFKA = Containers.kafka();

    static final long COMMIT_MS = 10;
    static final int[] RATES = {1_000, 5_000, 10_000, 20_000, 50_000, 0};
    static final Duration WARMUP = Duration.ofSeconds(2);
    static final Duration MEASURE = Duration.ofSeconds(5);
    static final int REPEATS = 3;
    static final String FILE = "q8_3_conversation_tx_throughput.csv";
    static final int MAX_RECORDS = 6_000_000;

    @Test
    void throughput() throws Exception {
        Bench.env("Q8-3");
        Bench.deleteIfExists(FILE);
        runOnce(1_000, 0);
        for (int rep = 1; rep <= REPEATS; rep++) {
            for (int rate : RATES) {
                runOnce(rate, rep);
            }
        }
    }

    void runOnce(int rate, int rep) throws Exception {
        String topic = "q8-tput-" + rate + "-" + rep + "-" + System.nanoTime();
        StoreFixture.createTopic(KAFKA.getBootstrapServers(), topic);
        byte[] value = new byte[100];
        long[] scheduled = new long[MAX_RECORDS];
        long[] committed = new long[MAX_RECORDS];
        List<long[]> txs = new ArrayList<>(); // {커밋 시각, 레코드 수, 커밋 호출 ns}
        int n = 0;
        try (var producer = new KafkaProducer<String, byte[]>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.LINGER_MS_CONFIG, 0,
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "q8-tput-" + System.nanoTime()))) {
            producer.initTransactions();
            long start = System.nanoTime() + 100_000_000L;
            long end = start + WARMUP.toNanos() + MEASURE.toNanos();
            long interval = rate > 0 ? 1_000_000_000L / rate : 0;
            producer.beginTransaction();
            long txStart = System.nanoTime();
            int txFirst = 0;
            while (n < MAX_RECORDS) {
                long sch = rate > 0 ? start + n * interval : System.nanoTime();
                if (sch >= end) {
                    break;
                }
                if (rate > 0) {
                    long wait;
                    while ((wait = sch - System.nanoTime()) > 0) {
                        LockSupport.parkNanos(Math.min(wait, 100_000));
                    }
                }
                scheduled[n] = sch;
                producer.send(new ProducerRecord<>(topic, "conv-1", value));
                n++;
                if (System.nanoTime() - txStart >= COMMIT_MS * 1_000_000L) {
                    long c0 = System.nanoTime();
                    producer.commitTransaction();
                    long c1 = System.nanoTime();
                    for (int j = txFirst; j < n; j++) {
                        committed[j] = c1;
                    }
                    txs.add(new long[] {c1, n - txFirst, c1 - c0});
                    txFirst = n;
                    producer.beginTransaction();
                    txStart = System.nanoTime();
                }
            }
            long c0 = System.nanoTime();
            producer.commitTransaction();
            long c1 = System.nanoTime();
            for (int j = txFirst; j < n; j++) {
                committed[j] = c1;
            }
            txs.add(new long[] {c1, n - txFirst, c1 - c0});
            long measureStart = start + WARMUP.toNanos();
            // 측정 구간: 커밋 시각이 [measureStart, end) 에 든 트랜잭션.
            long recs = 0, txCount = 0;
            List<Long> perTx = new ArrayList<>(), commitCall = new ArrayList<>();
            for (long[] t : txs) {
                if (t[0] >= measureStart && t[0] < end) {
                    recs += t[1];
                    txCount++;
                    perTx.add(t[1]);
                    commitCall.add(t[2]);
                }
            }
            List<Long> accept = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                if (scheduled[i] >= measureStart && scheduled[i] < end) {
                    accept.add(committed[i] - scheduled[i]);
                }
            }
            double sec = MEASURE.toNanos() / 1e9;
            long[] pt = perTx.stream().mapToLong(Long::longValue).sorted().toArray();
            Bench.Summary cs = Bench.Summary.of(commitCall.stream().mapToLong(Long::longValue).toArray());
            Bench.Summary as = Bench.Summary.of(accept.stream().mapToLong(Long::longValue).toArray());
            String line = String.format(Locale.ROOT, "%d,%d,%.1f,%.1f,%d,%.1f,%d,%.3f,%.3f,%.3f,%.3f", rate, rep,
                    recs / sec, txCount / sec, pt.length == 0 ? 0 : pt[pt.length / 2],
                    pt.length == 0 ? 0 : (double) recs / txCount, pt.length == 0 ? 0 : pt[pt.length - 1],
                    cs.p50(), cs.p99(), as.p50(), as.p99());
            System.out.println((rep > 0 ? "RESULT " : "WARMUP ") + line);
            if (rep > 0) {
                Bench.append(FILE, "offered_per_sec,rep,committed_records_per_sec,tx_per_sec,records_per_tx_p50,"
                        + "records_per_tx_mean,records_per_tx_max,commit_call_p50_ms,commit_call_p99_ms,accept_p50_ms,"
                        + "accept_p99_ms", line);
            }
        }
    }
}
