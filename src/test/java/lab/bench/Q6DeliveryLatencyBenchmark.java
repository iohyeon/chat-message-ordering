package lab.bench;

import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
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
 * Q6 (1). 전송부터 소비까지의 지연을 read_committed 와 read_uncommitted 소비자로 동시에 잰다.
 *
 * <p>producer 한 개가 1ms마다 레코드 하나를 보낸다(초당 1,000건). 트랜잭션 모드에서는 트랜잭션을 연 지
 * commitMs가 지나면 커밋하고 새 트랜잭션을 연다. 지연은 "보내기로 예정된 시각"부터 소비자가 poll로 받은 시각까지다.
 * commitTransaction()이 막는 동안 늦어진 전송도 지연에 포함하려는 것이다(coordinated omission 방지).
 * 실제 send() 호출 시각부터의 지연도 함께 기록한다. 시각은 같은 JVM의 System.nanoTime()이다.
 */
@Testcontainers
@Tag("benchmark")
class Q6DeliveryLatencyBenchmark {

    @Container
    static final KafkaContainer KAFKA = Containers.kafka();

    static final int RATE_PER_SEC = 1000;
    static final Duration WARMUP = Duration.ofSeconds(3);
    static final Duration MEASURE = Duration.ofSeconds(10);
    static final int REPEATS = 3;
    static final String SUMMARY = "q6_1_delivery_latency_summary.csv";
    static final String RAW = "q6_1_delivery_latency_raw.csv.gz";

    record Config(String producer, int lingerMs, int commitMs) {
        String label() {
            return producer + "/linger" + lingerMs + (commitMs > 0 ? "/commit" + commitMs : "");
        }
    }

    @Test
    void deliveryLatency() throws Exception {
        Bench.env("Q6-1");
        Bench.deleteIfExists(SUMMARY);
        List<Config> configs = List.of(
                new Config("idempotent", 0, 0),
                new Config("transactional", 0, 10),
                new Config("transactional", 0, 100),
                new Config("transactional", 0, 1000),
                new Config("idempotent", 5, 0),
                new Config("transactional", 5, 10),
                new Config("transactional", 5, 100),
                new Config("transactional", 5, 1000));
        // 한 번 버리는 실행으로 브로커와 JVM을 데운다.
        runOnce(new Config("transactional", 0, 100), -1, null);
        try (PrintWriter raw = Bench.gzipWriter(RAW,
                "config,rep,isolation,scheduled_at_ms,latency_from_schedule_us,latency_from_send_us,"
                        + "append_minus_send_wall_ms,receive_minus_append_wall_ms")) {
            // 반복을 바깥에 두어 설정 사이의 시간 흐름(열, 백그라운드 부하)이 한 설정에만 몰리지 않게 한다.
            for (int rep = 1; rep <= REPEATS; rep++) {
                for (Config c : configs) {
                    runOnce(c, rep, raw);
                }
            }
        }
    }

    void runOnce(Config c, int rep, PrintWriter raw) throws Exception {
        String topic = "lat-" + c.label().replace('/', '-') + "-" + rep + "-" + System.nanoTime();
        // 브로커가 레코드를 로그에 붙인 시각을 레코드 timestamp로 받는다. 지연을 "전송~브로커"와 "브로커~소비"로 나누려는 것이다.
        StoreFixture.createTopic(KAFKA.getBootstrapServers(), topic, Map.of("message.timestamp.type", "LogAppendTime"));
        long measureStart = System.nanoTime() + WARMUP.toNanos() + Duration.ofMillis(500).toNanos();
        long measureEnd = measureStart + MEASURE.toNanos();

        var committed = new Collector("read_committed", topic, measureStart, measureEnd);
        var uncommitted = new Collector("read_uncommitted", topic, measureStart, measureEnd);
        committed.start();
        uncommitted.start();
        committed.ready.await();
        uncommitted.ready.await();

        Bench.Gc gc0 = Bench.Gc.now();
        long commits = 0;
        long commitNanosTotal = 0, commitNanosMax = 0;
        boolean tx = c.producer().equals("transactional");
        Map<String, Object> props = new java.util.HashMap<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.LINGER_MS_CONFIG, c.lingerMs()));
        if (tx) {
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "lat-" + System.nanoTime());
        }
        long intervalNanos = 1_000_000_000L / RATE_PER_SEC;
        try (var producer = new KafkaProducer<byte[], byte[]>(props)) {
            if (tx) {
                producer.initTransactions();
                producer.beginTransaction();
            }
            long start = System.nanoTime();
            long txStart = start;
            long end = measureEnd + Duration.ofMillis(200).toNanos();
            for (long i = 0; ; i++) {
                long scheduled = start + i * intervalNanos;
                if (scheduled > end) {
                    break;
                }
                long wait;
                while ((wait = scheduled - System.nanoTime()) > 0) {
                    LockSupport.parkNanos(Math.min(wait, 200_000));
                }
                long sendAt = System.nanoTime();
                byte[] v = ByteBuffer.allocate(100).putLong(scheduled).putLong(sendAt)
                        .putLong(System.currentTimeMillis()).array();
                producer.send(new ProducerRecord<>(topic, v));
                if (tx && System.nanoTime() - txStart >= c.commitMs() * 1_000_000L) {
                    long t0 = System.nanoTime();
                    producer.commitTransaction();
                    long d = System.nanoTime() - t0;
                    commits++;
                    commitNanosTotal += d;
                    commitNanosMax = Math.max(commitNanosMax, d);
                    producer.beginTransaction();
                    txStart = System.nanoTime();
                }
            }
            if (tx) {
                producer.commitTransaction();
            } else {
                producer.flush();
            }
        }
        // 마지막 레코드가 read_committed에 보일 때까지 기다린 뒤 멈춘다.
        Thread.sleep(Math.max(1500, c.commitMs() * 2L));
        committed.stop.set(true);
        uncommitted.stop.set(true);
        committed.join();
        uncommitted.join();
        Bench.Gc gc = Bench.Gc.now().minus(gc0);

        for (Collector col : List.of(committed, uncommitted)) {
            long[] fromSchedule = col.fromSchedule.toArray();
            Bench.Summary s = Bench.Summary.of(fromSchedule);
            Bench.Summary sSend = Bench.Summary.of(col.fromSend.toArray());
            String line = String.format(Locale.ROOT, "%s,%s,%d,%d,%d,%s,%s,%.3f,%.3f,%d,%.1f,%.1f,%d,%d",
                    c.label(), c.producer(), c.lingerMs(), c.commitMs(), rep, col.isolation, s.csv(),
                    sSend.p50(), sSend.p99(), commits,
                    commits == 0 ? 0 : commitNanosTotal / 1e6 / commits, commitNanosMax / 1e6, gc.count(), gc.millis());
            System.out.println("RESULT " + line);
            if (rep > 0) {
                Bench.append(SUMMARY, "config,producer,linger_ms,commit_ms,rep,isolation," + Bench.Summary.HEADER
                        + ",from_send_p50_ms,from_send_p99_ms,commits,commit_call_mean_ms,commit_call_max_ms,gc_count,gc_ms", line);
                for (int i = 0; i < fromSchedule.length; i++) {
                    raw.printf(Locale.ROOT, "%s,%d,%s,%d,%d,%d,%d,%d%n", c.label(), rep, col.isolation,
                            (col.scheduledAt.get(i) - measureStart) / 1_000_000, fromSchedule[i] / 1000,
                            col.fromSend.get(i) / 1000, col.appendMinusSend.get(i), col.receiveMinusAppend.get(i));
                }
            }
        }
    }

    /** 소비자 스레드. 측정 구간에 예정된 레코드만 모은다. */
    static final class Collector extends Thread {
        final String isolation;
        final String topic;
        final long from, to;
        final LongList fromSchedule = new LongList();
        final LongList fromSend = new LongList();
        final LongList scheduledAt = new LongList();
        final LongList appendMinusSend = new LongList();
        final LongList receiveMinusAppend = new LongList();
        final AtomicBoolean stop = new AtomicBoolean();
        final CountDownLatch ready = new CountDownLatch(1);

        Collector(String isolation, String topic, long from, long to) {
            super("collector-" + isolation);
            this.isolation = isolation;
            this.topic = topic;
            this.from = from;
            this.to = to;
        }

        @Override
        public void run() {
            TopicPartition tp = new TopicPartition(topic, 0);
            try (var consumer = new KafkaConsumer<byte[], byte[]>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                    ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation,
                    ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1,
                    ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500))) {
                consumer.assign(Set.of(tp));
                consumer.seekToBeginning(Set.of(tp));
                consumer.poll(Duration.ZERO);
                ready.countDown();
                while (!stop.get()) {
                    var records = consumer.poll(Duration.ofMillis(100));
                    long now = System.nanoTime();
                    long nowWall = System.currentTimeMillis();
                    for (var r : records) {
                        ByteBuffer b = ByteBuffer.wrap(r.value());
                        long scheduled = b.getLong();
                        long sendAt = b.getLong();
                        long sendWall = b.getLong();
                        if (scheduled >= from && scheduled < to) {
                            fromSchedule.add(now - scheduled);
                            fromSend.add(now - sendAt);
                            scheduledAt.add(scheduled);
                            appendMinusSend.add(r.timestamp() - sendWall);
                            receiveMinusAppend.add(nowWall - r.timestamp());
                        }
                    }
                }
            }
        }
    }

    /** 박싱 없는 long 목록. */
    static final class LongList {
        long[] a = new long[16384];
        int n;

        void add(long v) {
            if (n == a.length) {
                a = java.util.Arrays.copyOf(a, n * 2);
            }
            a[n++] = v;
        }

        long get(int i) {
            return a[i];
        }

        long[] toArray() {
            return java.util.Arrays.copyOf(a, n);
        }
    }
}
