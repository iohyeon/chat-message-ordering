package lab.bench;

import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import lab.Containers;
import lab.store.ChatRecord;
import lab.store.FenceMode;
import lab.store.MessageStore;
import lab.store.StoreFixture;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
 * Q7 (3). 저장소 fencing에서 사용자 성공 응답을 저장 결과 뒤로 옮기면 수락 지연이 얼마나 느는가.
 *
 * <p>담당자 하나가 일정한 간격으로 레코드를 보낸다(멱등 producer, acks=all, linger.ms=0). 저장 워커 하나가 같은 파티션을
 * 계속 읽어 {@link FenceMode#LOG_ORDER} 로 저장한다. 레코드마다 세 시각을 잰다. 모두 "보내기로 예정된 시각"부터다
 * (Q6 (1)과 같이 coordinated omission을 피하려는 것이다).
 * <ul>
 *   <li>ack: 브로커 ack 콜백. 브로커 ack를 보고 응답하는 지금 구현의 수락 지연(기준선).
 *   <li>polled: 워커의 poll()이 그 레코드를 돌려준 시각.
 *   <li>stored: 워커가 그 레코드의 저장을 끝낸(커밋이 돌아온) 시각. 저장 결과를 보고 응답할 때 수락 지연의 하한이다.
 *       워커가 담당자에게 결과를 알리는 경로의 비용은 들어 있지 않다.
 * </ul>
 * 저장 워커는 두 가지다. per-record는 {@code StoreWorker} 처럼 레코드마다 autocommit 문장 하나, per-poll은 poll 한 번에 받은
 * 레코드를 트랜잭션 하나로 넣고 한 번 커밋한다. 부하는 초당 500, 1,000, 2,000, 4,000건. 대화 64개에 돌아가며 쓴다.
 */
@Testcontainers
@Tag("benchmark")
class Q7AcceptLatencyBenchmark {

    @Container
    static final KafkaContainer KAFKA = Containers.kafka();

    static final Duration WARMUP = Duration.ofSeconds(3);
    static final Duration MEASURE = Duration.ofSeconds(10);
    static final int REPEATS = 3;
    static final int CONVERSATIONS = 64;
    static final String SUMMARY = "q7_3_accept_latency_summary.csv";

    record Config(String worker, int rate) {
        String label() {
            return worker + "/" + rate;
        }
    }

    static final List<Config> CONFIGS = List.of(
            new Config("per-record", 500), new Config("per-record", 1000), new Config("per-record", 2000),
            new Config("per-record", 4000),
            new Config("per-poll", 500), new Config("per-poll", 1000), new Config("per-poll", 2000),
            new Config("per-poll", 4000));

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

    /**
     * 같은 실행 안에서 두 방식의 수락 지연을 번갈아 잰다. 초당 1,000건.
     * store/*: 위와 같은 저장소 fencing(수락 = 저장 완료). broker/tx-N: 트랜잭션 producer가 N ms마다 커밋하고
     * (수락 = 그 레코드가 든 트랜잭션의 commitTransaction()이 돌아온 시각), read_committed 소비자가 받은 시각도 함께 잰다.
     */
    @Test
    void sideBySideFsyncOn() throws Exception {
        try (PostgreSQLContainer pg = Containers.postgres()) {
            pg.setCommand("postgres", "-c", "fsync=on");
            pg.start();
            sideBySide(pg, "fsync-on");
        }
    }

    @Test
    void sideBySideFsyncOff() throws Exception {
        try (PostgreSQLContainer pg = Containers.postgres()) {
            pg.start();
            sideBySide(pg, "fsync-off");
        }
    }

    static final String SIDE = "q7_3_side_by_side.csv";

    void sideBySide(PostgreSQLContainer pg, String label) throws Exception {
        Bench.env("Q7-3 side-by-side " + label);
        try (HikariDataSource ds = StoreFixture.dataSource(pg, 4)) {
            StoreFixture.applySchema(ds);
            runOnce(ds, label, new Config("per-record", 1000), 0, null);
            runBrokerOnce(label, 100, 0);
            for (int rep = 1; rep <= REPEATS; rep++) {
                for (String path : List.of("store/per-record", "store/per-poll", "broker/tx-10", "broker/tx-100")) {
                    if (path.startsWith("store/")) {
                        long[][] t = runOnce(ds, label, new Config(path.substring(6), 1000), rep, null);
                        appendSide(label, path, rep, t[0], t[1], null);
                    } else {
                        runBrokerOnce(label, Integer.parseInt(path.substring(10)), rep);
                    }
                }
            }
        }
    }

    static void appendSide(String label, String path, int rep, long[] ack, long[] accept, long[] rc) {
        Bench.Summary a = Bench.Summary.of(ack), c = Bench.Summary.of(accept);
        Bench.Summary d = rc == null ? Bench.Summary.of(new long[0]) : Bench.Summary.of(rc);
        String line = String.format(Locale.ROOT, "%s,%s,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f", label, path,
                rep, ack.length, a.p50(), a.p99(), a.max(), c.p50(), c.p99(), c.max(), d.p50(), d.p99(), d.max());
        System.out.println((rep > 0 ? "SIDE " : "SIDE-WARMUP ") + line);
        if (rep > 0) {
            Bench.append(SIDE, "postgres,path,rep,n,ack_p50_ms,ack_p99_ms,ack_max_ms,accept_p50_ms,accept_p99_ms,"
                    + "accept_max_ms,read_committed_p50_ms,read_committed_p99_ms,read_committed_max_ms", line);
        }
    }

    /** 브로커 fencing 쪽. Q6 (1)의 트랜잭션 루프와 같지만 커밋이 돌아온 시각을 레코드마다 남긴다. */
    void runBrokerOnce(String label, int commitMs, int rep) throws Exception {
        String topic = "acc-broker-" + commitMs + "-" + rep + "-" + System.nanoTime();
        StoreFixture.createTopic(KAFKA.getBootstrapServers(), topic);
        long interval = 1_000_000L;
        long start = System.nanoTime() + 300_000_000L;
        long measureStart = start + WARMUP.toNanos();
        long measureEnd = measureStart + MEASURE.toNanos();
        int n = (int) ((measureEnd + 200_000_000L - start) / interval);
        long[] scheduled = new long[n], ack = new long[n], committed = new long[n], received = new long[n];
        for (int i = 0; i < n; i++) {
            scheduled[i] = start + i * interval;
        }
        AtomicBoolean stop = new AtomicBoolean();
        var ready = new java.util.concurrent.CountDownLatch(1);
        Thread consumerThread = new Thread(() -> {
            try (var consumer = new KafkaConsumer<String, String>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                    ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                    ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1,
                    ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500))) {
                TopicPartition tp = new TopicPartition(topic, 0);
                consumer.assign(Set.of(tp));
                consumer.seekToBeginning(Set.of(tp));
                consumer.poll(Duration.ZERO);
                ready.countDown();
                while (!stop.get()) {
                    var recs = consumer.poll(Duration.ofMillis(100));
                    long now = System.nanoTime();
                    for (var cr : recs) {
                        received[Integer.parseInt(cr.value().substring(2))] = now;
                    }
                }
            }
        }, "rc-consumer");
        consumerThread.start();
        ready.await();
        try (var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.LINGER_MS_CONFIG, 0,
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "acc-tx-" + System.nanoTime()))) {
            producer.initTransactions();
            producer.beginTransaction();
            long txStart = System.nanoTime();
            int txFirst = 0;
            for (int i = 0; i < n; i++) {
                long wait;
                while ((wait = scheduled[i] - System.nanoTime()) > 0) {
                    LockSupport.parkNanos(Math.min(wait, 200_000));
                }
                final int idx = i;
                producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, "k", "i=" + i),
                        (md, e) -> ack[idx] = e == null ? System.nanoTime() : -1);
                if (System.nanoTime() - txStart >= commitMs * 1_000_000L || i == n - 1) {
                    producer.commitTransaction();
                    long t = System.nanoTime();
                    for (int j = txFirst; j <= i; j++) {
                        committed[j] = t;
                    }
                    txFirst = i + 1;
                    if (i < n - 1) {
                        producer.beginTransaction();
                        txStart = System.nanoTime();
                    }
                }
            }
        }
        Thread.sleep(Math.max(1500, commitMs * 2L));
        stop.set(true);
        consumerThread.join();
        List<Long> a = new ArrayList<>(), c = new ArrayList<>(), d = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            if (scheduled[i] < measureStart || scheduled[i] >= measureEnd) {
                continue;
            }
            if (ack[i] <= 0 || committed[i] <= 0 || received[i] <= 0) {
                throw new IllegalStateException("시각 없음 i=" + i);
            }
            a.add(ack[i] - scheduled[i]);
            c.add(committed[i] - scheduled[i]);
            d.add(received[i] - scheduled[i]);
        }
        appendSide(label, "broker/tx-" + commitMs, rep, a.stream().mapToLong(Long::longValue).toArray(),
                c.stream().mapToLong(Long::longValue).toArray(), d.stream().mapToLong(Long::longValue).toArray());
    }

    void run(PostgreSQLContainer pg, String label) throws Exception {
        Bench.env("Q7-3 " + label);
        try (HikariDataSource ds = StoreFixture.dataSource(pg, 4);
             PrintWriter raw = Bench.gzipWriter("q7_3_accept_latency_raw_" + label + ".csv.gz",
                     "postgres,worker,rate,rep,scheduled_at_ms,ack_us,polled_us,stored_us")) {
            StoreFixture.applySchema(ds);
            // 한 번 버리는 실행으로 브로커, DB, JVM을 데운다.
            runOnce(ds, label, new Config("per-record", 1000), 0, null);
            for (int rep = 1; rep <= REPEATS; rep++) {
                for (Config c : CONFIGS) {
                    runOnce(ds, label, c, rep, raw);
                }
            }
        }
    }

    long[][] runOnce(HikariDataSource ds, String label, Config cfg, int rep, PrintWriter raw) throws Exception {
        StoreFixture.truncate(ds);
        String prefix = "acc-" + rep + "-" + System.nanoTime() + "-";
        try (Connection c = ds.getConnection(); var st = c.createStatement()) {
            st.execute("INSERT INTO message_fence SELECT '" + prefix + "' || g, 1 FROM generate_series(0, "
                    + (CONVERSATIONS - 1) + ") g");
            st.execute("VACUUM ANALYZE message_fence, message");
        }
        String topic = "acc-" + cfg.worker() + "-" + cfg.rate() + "-" + rep + "-" + System.nanoTime();
        StoreFixture.createTopic(KAFKA.getBootstrapServers(), topic);

        long interval = 1_000_000_000L / cfg.rate();
        long start = System.nanoTime() + 300_000_000L;
        long measureStart = start + WARMUP.toNanos();
        long measureEnd = measureStart + MEASURE.toNanos();
        int n = (int) ((measureEnd + 200_000_000L - start) / interval);
        long[] scheduled = new long[n];
        long[] ack = new long[n];
        long[] polled = new long[n];
        long[] stored = new long[n];
        for (int i = 0; i < n; i++) {
            scheduled[i] = start + i * interval;
        }

        var worker = new Worker(topic, ds, cfg.worker().equals("per-poll"), polled, stored, n);
        worker.start();
        worker.ready.await();
        Bench.Gc gc0 = Bench.Gc.now();
        long[] seqs = new long[CONVERSATIONS];
        try (var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.LINGER_MS_CONFIG, 0))) {
            for (int i = 0; i < n; i++) {
                long wait;
                while ((wait = scheduled[i] - System.nanoTime()) > 0) {
                    LockSupport.parkNanos(Math.min(wait, 200_000));
                }
                int conv = i % CONVERSATIONS;
                ChatRecord r = ChatRecord.message(prefix + conv, ++seqs[conv], 1, "i=" + i);
                final int idx = i;
                producer.send(r.toProducerRecord(topic), (md, e) -> ack[idx] = e == null ? System.nanoTime() : -1);
            }
            producer.flush();
        }
        long sendDone = System.nanoTime();
        worker.awaitAll(Duration.ofSeconds(180));
        long drainDone = System.nanoTime();
        worker.close();
        Bench.Gc gc = Bench.Gc.now().minus(gc0);

        List<Long> fAck = new ArrayList<>(), fPolled = new ArrayList<>(), fStored = new ArrayList<>();
        int measured = 0;
        for (int i = 0; i < n; i++) {
            if (scheduled[i] < measureStart || scheduled[i] >= measureEnd) {
                continue;
            }
            if (ack[i] <= 0 || stored[i] <= 0) {
                throw new IllegalStateException("시각 없음 i=" + i + " ack=" + ack[i] + " stored=" + stored[i]);
            }
            measured++;
            fAck.add(ack[i] - scheduled[i]);
            fPolled.add(polled[i] - scheduled[i]);
            fStored.add(stored[i] - scheduled[i]);
            if (raw != null) {
                raw.printf(Locale.ROOT, "%s,%s,%d,%d,%d,%d,%d,%d%n", label, cfg.worker(), cfg.rate(), rep,
                        (scheduled[i] - measureStart) / 1_000_000, (ack[i] - scheduled[i]) / 1000,
                        (polled[i] - scheduled[i]) / 1000, (stored[i] - scheduled[i]) / 1000);
            }
        }
        Bench.Summary sAck = Bench.Summary.of(fAck.stream().mapToLong(Long::longValue).toArray());
        Bench.Summary sPolled = Bench.Summary.of(fPolled.stream().mapToLong(Long::longValue).toArray());
        Bench.Summary sStored = Bench.Summary.of(fStored.stream().mapToLong(Long::longValue).toArray());
        double workerRate = worker.processedInMeasure(measureStart, measureEnd) / (MEASURE.toNanos() / 1e9);
        String line = String.format(Locale.ROOT, "%s,%s,%d,%d,%d,%s,%.3f,%.3f,%s,%.1f,%.1f,%.3f,%d,%d", label,
                cfg.worker(), cfg.rate(), rep, measured, sAck.csv(), sPolled.p50(), sPolled.p99(), sStored.csv(),
                workerRate, worker.batches.get() == 0 ? 0 : (double) worker.records.get() / worker.batches.get(),
                (drainDone - sendDone) / 1e6, gc.count(), gc.millis());
        System.out.println((rep > 0 ? "RESULT " : "WARMUP ") + line);
        if (rep > 0 && raw != null) {
            Bench.append(SUMMARY, "postgres,worker,rate,rep,measured,ack_n,ack_p50_ms,ack_p90_ms,ack_p99_ms,ack_max_ms,"
                    + "ack_mean_ms,polled_p50_ms,polled_p99_ms,stored_n,stored_p50_ms,stored_p90_ms,stored_p99_ms,"
                    + "stored_max_ms,stored_mean_ms,"
                    + "worker_records_per_sec_in_window,records_per_poll,drain_after_send_ms,gc_count,gc_ms", line);
        }
        return new long[][] {fAck.stream().mapToLong(Long::longValue).toArray(),
                fStored.stream().mapToLong(Long::longValue).toArray()};
    }

    /** 저장 워커. 레코드 본문의 번호로 시각 배열에 기록한다. */
    static final class Worker extends Thread implements AutoCloseable {
        final String topic;
        final HikariDataSource ds;
        final boolean perPoll;
        final long[] polled, stored;
        final int total;
        final AtomicLong done = new AtomicLong();
        final AtomicLong batches = new AtomicLong();
        final AtomicLong records = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean();
        final java.util.concurrent.CountDownLatch ready = new java.util.concurrent.CountDownLatch(1);
        volatile Throwable failure;

        Worker(String topic, HikariDataSource ds, boolean perPoll, long[] polled, long[] stored, int total) {
            super("accept-worker");
            this.topic = topic;
            this.ds = ds;
            this.perPoll = perPoll;
            this.polled = polled;
            this.stored = stored;
            this.total = total;
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
                c.setAutoCommit(!perPoll);
                consumer.assign(Set.of(tp));
                consumer.seekToBeginning(Set.of(tp));
                consumer.poll(Duration.ZERO);
                ready.countDown();
                while (!stop.get()) {
                    var recs = consumer.poll(Duration.ofMillis(100));
                    if (recs.isEmpty()) {
                        continue;
                    }
                    long now = System.nanoTime();
                    batches.incrementAndGet();
                    List<Integer> pending = new ArrayList<>();
                    for (ConsumerRecord<String, String> cr : recs) {
                        ChatRecord r = ChatRecord.from(cr);
                        int idx = Integer.parseInt(r.body().substring(2));
                        polled[idx] = now;
                        int ins = MessageStore.insert(c, FenceMode.LOG_ORDER, r);
                        if (ins != 1) {
                            throw new IllegalStateException("0행 " + r);
                        }
                        if (perPoll) {
                            pending.add(idx);
                        } else {
                            stored[idx] = System.nanoTime();
                        }
                        records.incrementAndGet();
                    }
                    if (perPoll) {
                        c.commit();
                        long t = System.nanoTime();
                        for (int idx : pending) {
                            stored[idx] = t;
                        }
                    }
                    done.addAndGet(recs.count());
                }
            } catch (Throwable e) {
                failure = e;
            } finally {
                ready.countDown();
            }
        }

        void awaitAll(Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (done.get() < total) {
                if (failure != null) {
                    throw new IllegalStateException("워커 실패", failure);
                }
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("워커가 끝나지 않음: " + done.get() + "/" + total);
                }
                Thread.sleep(10);
            }
        }

        /** 측정 구간 안에 저장을 끝낸 레코드 수(워커가 실제로 낸 처리량). */
        long processedInMeasure(long from, long to) {
            long k = 0;
            for (long t : stored) {
                if (t >= from && t < to) {
                    k++;
                }
            }
            return k;
        }

        @Override
        public void close() throws InterruptedException {
            stop.set(true);
            join();
        }
    }
}
