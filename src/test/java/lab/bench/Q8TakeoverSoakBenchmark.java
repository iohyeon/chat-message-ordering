package lab.bench;

import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import lab.Containers;
import lab.lease.LeaseRepository;
import lab.store.ChatRecord;
import lab.store.CommittedReplay;
import lab.store.MessageStore;
import lab.store.Q8Support;
import lab.store.Q8Support.WorkerKey;
import lab.store.StoreFixture;
import lab.store.StoreWorker;
import lab.store.TxEpochWriter;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.IsolationLevel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Q8 (2). 브로커 fencing에 저장소 세대 검사를 더한 구성에서 담당 교체를 여러 번 일으켜, 비교 방식별로
 * "성공 응답했지만 저장되지 않은 레코드", "순번 겹침", "빈자리"를 센다.
 *
 * <p>교체 한 번: 현재 담당자 A가 트랜잭션을 열고 레코드 {@link #K}개를 보낸 뒤(flush, 커밋 전) 멈춘다. 기준 시각 t0에
 * B가 lease를 만료시키고 획득한 뒤, 트랜잭션 producer를 만들고 initTransactions(), 교체 표시 커밋, read_committed replay,
 * 레코드 K개 커밋을 한다. A는 t0 + dA에 커밋한다. dA는 [-100ms, 300ms) 에서 고르게 뽑는다. 그래서 A의 커밋은
 * lease 교체 전, lease 교체 뒤 init 전, init 뒤(거부) 중 어디에나 떨어진다. B의 수락이 끝나면 A가 커밋에 성공했던 경우
 * 한 번 더 보내 본다(좀비 전송). 다음 교체에서는 B가 A가 된다.
 *
 * <p>저장 워커 8개(Q8Support.keys())가 같은 로그를 read_committed 로 처리한다. 따라잡은 워커 4개는 각자 스레드에서
 * 계속 poll하고, 밀린 워커 4개는 교체 한 번이 끝날 때마다 그때까지의 로그를 처리한다.
 */
@Testcontainers
@Tag("benchmark")
class Q8TakeoverSoakBenchmark {

    @Container
    static final KafkaContainer KAFKA = Containers.kafka();

    @Container
    static final PostgreSQLContainer POSTGRES = Containers.postgres();

    static final int TAKEOVERS = 300;
    static final int RUNS = 2;
    static final int K = 2;
    static final long DA_MIN_MS = -100;
    static final long DA_MAX_MS = 300;
    static final String SUMMARY = "q8_takeover_soak.csv";

    enum Group { BEFORE_LEASE, OVERLAP_LEASE, AFTER_LEASE_BEFORE_INIT, FENCED }

    record Takeover(int iteration, long epochA, long epochB, long daMs, Group group, boolean aOk, String aErr,
                    long aStartUs, long aEndUs, long leaseStartUs, long leaseEndUs, long initStartUs, long initEndUs,
                    long replayLastSeq, long aFirstSeq, long bFirstSeq, String zombie, List<ChatRecord> aRecords,
                    long t0Nanos) {
    }

    @Test
    void soak() throws Exception {
        Bench.env("Q8-2 soak");
        Bench.deleteIfExists(SUMMARY);
        try (HikariDataSource publicDs = Q8Support.dataSource(POSTGRES, null, 4);
             Admin admin = Q8Support.admin(KAFKA.getBootstrapServers());
             PrintWriter rawTakeover = Bench.gzipWriter("q8_takeover_soak_takeovers.csv.gz",
                     "run,iteration,epoch_a,epoch_b,da_ms,group,a_commit_ok,a_commit_error,a_commit_start_us,"
                             + "a_commit_end_us,lease_start_us,lease_end_us,init_start_us,init_end_us,replay_last_seq,"
                             + "a_first_seq,b_first_seq,zombie_send");
             PrintWriter rawRecord = Bench.gzipWriter("q8_takeover_soak_records.csv.gz", recordHeader())) {
            Q8Support.createSchemas(publicDs, Q8Support.keys());
            try (var c = publicDs.getConnection(); var rs = c.createStatement().executeQuery("SHOW fsync")) {
                rs.next();
                System.out.println("PGSETTING fsync=" + rs.getString(1));
            }
            Map<WorkerKey, HikariDataSource> ds = new LinkedHashMap<>();
            for (WorkerKey k : Q8Support.keys()) {
                ds.put(k, Q8Support.dataSource(POSTGRES, k.schema(), 2));
            }
            try {
                for (int run = 1; run <= RUNS; run++) {
                    runOnce(run, publicDs, ds, admin, rawTakeover, rawRecord);
                }
            } finally {
                ds.values().forEach(HikariDataSource::close);
            }
        }
    }

    static String recordHeader() {
        StringBuilder sb = new StringBuilder("run,iteration,group,a_commit_ok,epoch,seq,body");
        for (WorkerKey k : Q8Support.keys()) {
            sb.append(",stored_").append(k.csvLabel().replace('/', '_').toLowerCase(Locale.ROOT));
        }
        for (WorkerKey k : Q8Support.keys()) {
            sb.append(",processed_us_").append(k.csvLabel().replace('/', '_').toLowerCase(Locale.ROOT));
        }
        return sb.toString();
    }

    void runOnce(int run, HikariDataSource publicDs, Map<WorkerKey, HikariDataSource> ds, Admin admin,
                 PrintWriter rawTakeover, PrintWriter rawRecord) throws Exception {
        String id = run + "-" + System.nanoTime();
        String topic = "q8-soak-" + id;
        String conv = "conv-soak-" + id;
        String txId = "tx-" + conv;
        String bootstrap = KAFKA.getBootstrapServers();
        StoreFixture.createTopic(bootstrap, topic);
        LeaseRepository leases = new LeaseRepository(publicDs);
        leases.create(conv, 0);
        Random rnd = new Random(run * 7919L);

        Map<WorkerKey, StoreWorker> workers = new LinkedHashMap<>();
        Map<WorkerKey, ConcurrentLinkedQueue<StoreWorker.Processed>> processed = new LinkedHashMap<>();
        Map<WorkerKey, AtomicLong> positions = new LinkedHashMap<>();
        for (WorkerKey k : Q8Support.keys()) {
            workers.put(k, new StoreWorker(bootstrap, topic, ds.get(k), k.mode(), "read_committed"));
            processed.put(k, new ConcurrentLinkedQueue<>());
            positions.put(k, new AtomicLong());
        }
        AtomicBoolean stop = new AtomicBoolean();
        List<Thread> threads = new ArrayList<>();
        ConcurrentLinkedQueue<Throwable> workerErrors = new ConcurrentLinkedQueue<>();
        for (WorkerKey k : Q8Support.keys()) {
            if (k.lagging()) {
                continue;
            }
            Thread t = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        processed.get(k).addAll(workers.get(k).pollOnce(Duration.ofMillis(20)));
                        positions.get(k).set(workers.get(k).position());
                    }
                } catch (Throwable e) {
                    workerErrors.add(e);
                }
            }, "worker-" + k.schema());
            t.start();
            threads.add(t);
        }
        ExecutorService aThread = Executors.newSingleThreadExecutor();
        List<Takeover> takeovers = new ArrayList<>();
        List<TxEpochWriter> allWriters = new ArrayList<>();
        List<ChatRecord> initialRecords = new ArrayList<>();
        long t00 = System.nanoTime();
        try {
            long epoch = leases.tryAcquire(conv, "w0", Duration.ofSeconds(30)).orElseThrow();
            TxEpochWriter current = new TxEpochWriter("w0", bootstrap, topic, conv, txId, epoch, Map.of());
            allWriters.add(current);
            current.initTransactions();
            current.startAt(1);
            List<ChatRecord> initial = current.accept(List.of("e" + epoch + "-s1"));
            // 밀린 워커도 첫 교체 전의 레코드는 교체 전에 처리한다(JUnit의 seq 100과 같은 조건).
            Q8Support.awaitStable(admin, topic);
            for (WorkerKey k : Q8Support.keys()) {
                if (k.lagging()) {
                    processed.get(k).addAll(workers.get(k).drainToEnd(Duration.ofSeconds(30)));
                }
            }
            initialRecords.addAll(initial);
            for (int i = 0; i < TAKEOVERS; i++) {
                TxEpochWriter a = current;
                long seqA = a.nextSeq();
                List<String> aBodies = new ArrayList<>();
                for (int j = 0; j < K; j++) {
                    aBodies.add("e" + a.epoch() + "-s" + (seqA + j));
                }
                List<ChatRecord> aRecords = a.beginAndSend(aBodies);
                long daMs = DA_MIN_MS + (long) (rnd.nextDouble() * (DA_MAX_MS - DA_MIN_MS));
                long t0 = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(-DA_MIN_MS + 5);
                long[] aTimes = new long[2];
                CompletableFuture<String> aCommit = CompletableFuture.supplyAsync(() -> {
                    parkUntil(t0 + TimeUnit.MILLISECONDS.toNanos(daMs));
                    aTimes[0] = System.nanoTime();
                    try {
                        a.commit();
                        return null;
                    } catch (RuntimeException e) {
                        return e.getClass().getSimpleName();
                    } finally {
                        aTimes[1] = System.nanoTime();
                    }
                }, aThread);

                parkUntil(t0);
                long leaseStart = System.nanoTime();
                leases.forceExpire(conv);
                long epochB = leases.tryAcquire(conv, "w" + (i + 1), Duration.ofSeconds(30)).orElseThrow();
                long leaseEnd = System.nanoTime();
                TxEpochWriter b = new TxEpochWriter("w" + (i + 1), bootstrap, topic, conv, txId, epochB, Map.of());
                allWriters.add(b);
                long initStart = System.nanoTime();
                b.initTransactions();
                long initEnd = System.nanoTime();
                b.writeMarker();
                CommittedReplay.Result replay = CommittedReplay.replay(bootstrap, topic, "replay-" + id + "-" + i,
                        Duration.ofSeconds(30));
                long bFirst = replay.lastSeq() + 1;
                b.startAt(bFirst);
                List<String> bBodies = new ArrayList<>();
                for (int j = 0; j < K; j++) {
                    bBodies.add("e" + epochB + "-s" + (bFirst + j));
                }
                b.accept(bBodies);
                String aErr = aCommit.get(60, TimeUnit.SECONDS);
                boolean aOk = aErr == null;

                String zombie = "-";
                if (aOk) {
                    try {
                        a.beginAndSend(List.of("e" + a.epoch() + "-zombie" + i));
                        a.commit();
                        zombie = "accepted";
                    } catch (Exception e) {
                        Throwable c = e instanceof java.util.concurrent.ExecutionException && e.getCause() != null
                                ? e.getCause() : e;
                        zombie = c.getClass().getSimpleName();
                    }
                }
                a.close();

                Group g;
                if (!aOk) {
                    g = Group.FENCED;
                } else if (aTimes[1] < leaseStart) {
                    g = Group.BEFORE_LEASE;
                } else if (aTimes[0] > leaseEnd) {
                    g = Group.AFTER_LEASE_BEFORE_INIT;
                } else {
                    g = Group.OVERLAP_LEASE;
                }
                takeovers.add(new Takeover(i, a.epoch(), epochB, daMs, g, aOk, aErr, us(aTimes[0], t0), us(aTimes[1], t0),
                        us(leaseStart, t0), us(leaseEnd, t0), us(initStart, t0), us(initEnd, t0), replay.lastSeq(),
                        seqA, bFirst, zombie, aRecords, t0));

                Q8Support.awaitStable(admin, topic);
                for (WorkerKey k : Q8Support.keys()) {
                    if (k.lagging()) {
                        processed.get(k).addAll(workers.get(k).drainToEnd(Duration.ofSeconds(30)));
                    }
                }
                current = b;
                if ((i + 1) % 50 == 0) {
                    System.out.printf(Locale.ROOT, "PROGRESS run=%d takeovers=%d elapsed=%.1fs%n", run, i + 1,
                            (System.nanoTime() - t00) / 1e9);
                }
            }
            current.close();
            // 따라잡은 워커가 로그 끝(LSO)까지 처리할 때까지 기다린다.
            long[] end = Q8Support.awaitStable(admin, topic);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            for (WorkerKey k : Q8Support.keys()) {
                if (k.lagging()) {
                    continue;
                }
                while (positions.get(k).get() < end[1] && System.nanoTime() < deadline) {
                    Thread.sleep(20);
                }
                if (positions.get(k).get() < end[1]) {
                    throw new IllegalStateException(k.label() + " 가 끝(" + end[1] + ")까지 처리하지 못했다");
                }
            }
        } finally {
            stop.set(true);
            for (Thread t : threads) {
                t.join(10_000);
            }
            aThread.shutdownNow();
            workers.values().forEach(StoreWorker::close);
        }
        if (!workerErrors.isEmpty()) {
            throw new IllegalStateException("워커 오류", workerErrors.peek());
        }
        double seconds = (System.nanoTime() - t00) / 1e9;
        summarize(run, conv, topic, ds, takeovers, initialRecords, processed, rawTakeover, rawRecord, seconds);
    }

    static long us(long t, long t0) {
        return (t - t0) / 1000;
    }

    static void parkUntil(long t) {
        long wait;
        while ((wait = t - System.nanoTime()) > 0) {
            LockSupport.parkNanos(Math.min(wait, 200_000));
        }
    }

    void summarize(int run, String conv, String topic, Map<WorkerKey, HikariDataSource> ds, List<Takeover> takeovers,
                   List<ChatRecord> initialRecords,
                   Map<WorkerKey, ConcurrentLinkedQueue<StoreWorker.Processed>> processed, PrintWriter rawTakeover,
                   PrintWriter rawRecord, double seconds) throws Exception {
        List<ChatRecord> log = CommittedReplay.readAll(KAFKA.getBootstrapServers(), topic, Duration.ofSeconds(60));
        Map<Long, List<String>> dup = Q8Support.duplicateSeqsInLog(log);
        long zombieAccepted = takeovers.stream().filter(t -> t.zombie().equals("accepted")).count();
        Map<String, Long> zombieKinds = new java.util.TreeMap<>();
        takeovers.forEach(t -> zombieKinds.merge(t.zombie(), 1L, Long::sum));
        Map<Group, Long> groupCounts = new java.util.EnumMap<>(Group.class);
        takeovers.forEach(t -> groupCounts.merge(t.group(), 1L, Long::sum));
        System.out.printf(Locale.ROOT, "RUN %d takeovers=%d seconds=%.1f groups=%s logDuplicateSeqs=%d zombieSend=%s%n",
                run, takeovers.size(), seconds, groupCounts, dup.size(), zombieKinds);

        Map<WorkerKey, Map<Long, MessageStore.Row>> rows = new LinkedHashMap<>();
        Map<WorkerKey, java.util.Set<String>> storedBodies = new LinkedHashMap<>();
        for (WorkerKey k : Q8Support.keys()) {
            var r = Q8Support.rows(ds.get(k), conv);
            rows.put(k, r);
            storedBodies.put(k, r.values().stream().map(MessageStore.Row::body)
                    .collect(java.util.stream.Collectors.toSet()));
        }
        Map<WorkerKey, Map<String, Long>> processedAt = new LinkedHashMap<>();
        for (WorkerKey k : Q8Support.keys()) {
            Map<String, Long> m = new HashMap<>();
            for (StoreWorker.Processed p : processed.get(k)) {
                if (!p.record().marker()) {
                    m.putIfAbsent(p.record().body(), p.processedAtNanos());
                }
            }
            processedAt.put(k, m);
        }
        for (Takeover t : takeovers) {
            rawTakeover.printf(Locale.ROOT, "%d,%d,%d,%d,%d,%s,%d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s%n", run, t.iteration(),
                    t.epochA(), t.epochB(), t.daMs(), t.group(), t.aOk() ? 1 : 0, t.aErr() == null ? "" : t.aErr(),
                    t.aStartUs(), t.aEndUs(), t.leaseStartUs(), t.leaseEndUs(), t.initStartUs(), t.initEndUs(),
                    t.replayLastSeq(), t.aFirstSeq(), t.bFirstSeq(), t.zombie());
            for (ChatRecord rec : t.aRecords()) {
                StringBuilder sb = new StringBuilder(String.format(Locale.ROOT, "%d,%d,%s,%d,%d,%d,%s", run, t.iteration(),
                        t.group(), t.aOk() ? 1 : 0, rec.epoch(), rec.seq(), rec.body()));
                for (WorkerKey k : Q8Support.keys()) {
                    sb.append(',').append(storedBodies.get(k).contains(rec.body()) ? 1 : 0);
                }
                for (WorkerKey k : Q8Support.keys()) {
                    Long at = processedAt.get(k).get(rec.body());
                    sb.append(',').append(at == null ? "" : Long.toString(us(at, t.t0Nanos())));
                }
                rawRecord.println(sb);
            }
        }
        // B가 성공 응답한 레코드: 각 교체의 B 레코드 본문은 "e<epochB>-s<seq>" 형식이다.
        for (WorkerKey k : Q8Support.keys()) {
            var set = storedBodies.get(k);
            long slotTaken = processed.get(k).stream().filter(p -> p.outcome() == MessageStore.Outcome.SLOT_TAKEN).count();
            long duplicate = processed.get(k).stream().filter(p -> p.outcome() == MessageStore.Outcome.DUPLICATE).count();
            // 대화의 첫 순번은 1이다. 맨 앞이 빈 경우까지 센다.
            int gaps = Q8Support.gaps(rows.get(k), 1).size();
            long initialNotStored = initialRecords.stream().filter(x -> !set.contains(x.body())).count();
            Map<Group, long[]> byGroup = new java.util.EnumMap<>(Group.class);
            long bAcked = 0, bNotStored = 0;
            for (Takeover t : takeovers) {
                long[] c = byGroup.computeIfAbsent(t.group(), x -> new long[5]);
                c[0]++;
                for (ChatRecord rec : t.aRecords()) {
                    boolean stored = set.contains(rec.body());
                    if (t.aOk()) {
                        c[1]++;
                        if (!stored) {
                            c[2]++;
                        }
                    } else {
                        c[3]++;
                        if (stored) {
                            c[4]++;
                        }
                    }
                }
                for (int j = 0; j < K; j++) {
                    bAcked++;
                    if (!set.contains("e" + t.epochB() + "-s" + (t.bFirstSeq() + j))) {
                        bNotStored++;
                    }
                }
            }
            for (var e : byGroup.entrySet()) {
                long[] c = e.getValue();
                String line = String.format(Locale.ROOT, "%d,%s,%s,%d,%d,%d,%d,%d,,,,,,,", run, k.csvLabel(), e.getKey(),
                        c[0], c[1], c[2], c[3], c[4]);
                System.out.println("RESULT " + line);
                Bench.append(SUMMARY, summaryHeader(), line);
            }
            long[] tot = new long[5];
            byGroup.values().forEach(c -> {
                for (int x = 0; x < 5; x++) {
                    tot[x] += c[x];
                }
            });
            String line = String.format(Locale.ROOT, "%d,%s,ALL,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d", run, k.csvLabel(),
                    tot[0], tot[1], tot[2], tot[3], tot[4], bAcked, bNotStored, slotTaken, duplicate, gaps, dup.size(),
                    initialNotStored);
            System.out.println("RESULT " + line);
            Bench.append(SUMMARY, summaryHeader(), line);
        }
        if (zombieAccepted > 0) {
            System.out.println("WARN 좀비 전송이 받아들여진 교체: " + zombieAccepted);
        }
    }

    static String summaryHeader() {
        return "run,worker,group,takeovers,a_acked,a_acked_not_stored,a_failed,a_failed_but_stored,b_acked,"
                + "b_acked_not_stored,slot_taken,duplicate,gaps,log_duplicate_seqs,initial_not_stored";
    }
}
