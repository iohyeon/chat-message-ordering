package lab.bench;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import lab.Containers;
import lab.store.ChatRecord;
import lab.store.FenceMode;
import lab.store.MessageStore;
import lab.store.StoreFixture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Q6 (3). 단순 INSERT와 조건부 INSERT의 초당 처리 수. 연결(스레드) 1, 8, 32개. 각 문장은 autocommit이다.
 *
 * <p>대화 분포 두 가지: hot(모든 스레드가 대화 하나에 쓴다, lease 행 하나에 몰린다)과
 * spread(대화 1,024개 중 무작위). 순번은 전역 카운터라 키 충돌은 없다. 모든 레코드의 epoch는 현재 lease와 같아
 * 조건부 INSERT도 전부 성공하는 경로를 잰다. 측정 중 100ms마다 pg_stat_activity의 대기 이벤트를 표본으로 모은다.
 */
@Tag("benchmark")
class Q6StoreInsertBenchmark {

    static final Duration WARMUP = Duration.ofSeconds(2);
    static final Duration MEASURE = Duration.ofSeconds(8);
    static final int REPEATS = 3;
    static final int SPREAD_CONVERSATIONS = 1024;
    String summary;
    String waitsFile;

    /**
     * Testcontainers의 PostgreSQL 컨테이너는 기본 명령이 {@code postgres -c fsync=off} 다. 이 설정 그대로 한 번 잰다.
     */
    @Test
    void fsyncOff() throws Exception {
        try (PostgreSQLContainer pg = Containers.postgres()) {
            pg.start();
            run(pg, "fsync-off");
        }
    }

    /** 커밋마다 WAL을 디스크에 flush하는 PostgreSQL 기본값(fsync=on)으로 다시 잰다. 디스크는 colima VM의 가상 디스크다. */
    @Test
    void fsyncOn() throws Exception {
        try (PostgreSQLContainer pg = Containers.postgres()) {
            pg.setCommand("postgres", "-c", "fsync=on");
            pg.start();
            run(pg, "fsync-on");
        }
    }

    void run(PostgreSQLContainer pg, String label) throws Exception {
        Bench.env("Q6-3 " + label);
        summary = "q6_3_store_insert_" + label + ".csv";
        waitsFile = "q6_3_store_insert_waits_" + label + ".csv";
        Bench.deleteIfExists(summary);
        Bench.deleteIfExists(waitsFile);
        try (HikariDataSource ds = StoreFixture.dataSource(pg, 34)) {
            StoreFixture.applySchema(ds);
            printSettings(ds);
            List<FenceMode> modes = List.of(FenceMode.PLAIN, FenceMode.LEASE_EQ, FenceMode.LEASE_EQ_FOR_SHARE,
                    FenceMode.LOG_ORDER);
            runOnce(ds, FenceMode.LEASE_EQ, 8, "spread", 0);
            for (int rep = 1; rep <= REPEATS; rep++) {
                for (String dist : List.of("hot", "spread")) {
                    for (int conns : new int[] {1, 8, 32}) {
                        for (FenceMode mode : modes) {
                            runOnce(ds, mode, conns, dist, rep);
                        }
                    }
                }
            }
        }
    }

    static void printSettings(HikariDataSource ds) throws Exception {
        try (Connection c = ds.getConnection(); var st = c.createStatement()) {
            for (String s : List.of("server_version", "shared_buffers", "synchronous_commit", "fsync", "wal_level",
                    "max_connections", "default_transaction_isolation")) {
                try (var rs = st.executeQuery("SHOW " + s)) {
                    rs.next();
                    System.out.println("PGSETTING " + s + "=" + rs.getString(1));
                }
            }
        }
    }

    void runOnce(HikariDataSource ds, FenceMode mode, int conns, String dist, int rep) throws Exception {
        StoreFixture.truncate(ds);
        int convCount = dist.equals("hot") ? 1 : SPREAD_CONVERSATIONS;
        try (Connection c = ds.getConnection(); var st = c.createStatement()) {
            st.execute("INSERT INTO conversation_owner SELECT 'c' || g, 1, 'owner', now() + interval '1 hour' "
                    + "FROM generate_series(0, " + (convCount - 1) + ") g");
            st.execute("INSERT INTO message_fence SELECT 'c' || g, 1 FROM generate_series(0, " + (convCount - 1) + ") g");
            st.execute("VACUUM ANALYZE conversation_owner, message_fence, message");
        }
        AtomicLong seq = new AtomicLong();
        AtomicBoolean measuring = new AtomicBoolean();
        AtomicBoolean stop = new AtomicBoolean();
        LongAdder ok = new LongAdder();
        LongAdder zero = new LongAdder();
        LongAdder errors = new LongAdder();
        long[][] lat = new long[conns][];
        int[] latN = new int[conns];
        CountDownLatch done = new CountDownLatch(conns);
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < conns; t++) {
            final int ti = t;
            lat[t] = new long[1 << 16];
            Thread th = new Thread(() -> {
                try (Connection c = ds.getConnection()) {
                    while (!stop.get()) {
                        String conv = "c" + (convCount == 1 ? 0 : ThreadLocalRandom.current().nextInt(convCount));
                        ChatRecord r = ChatRecord.message(conv, seq.incrementAndGet(), 1, "body-0123456789-0123456789");
                        long t0 = System.nanoTime();
                        int n;
                        try {
                            n = MessageStore.insert(c, mode, r);
                        } catch (Exception e) {
                            errors.increment();
                            continue;
                        }
                        long d = System.nanoTime() - t0;
                        if (measuring.get()) {
                            if (n == 1) {
                                ok.increment();
                            } else {
                                zero.increment();
                            }
                            if (latN[ti] == lat[ti].length) {
                                lat[ti] = java.util.Arrays.copyOf(lat[ti], lat[ti].length * 2);
                            }
                            lat[ti][latN[ti]++] = d;
                        }
                    }
                } catch (Exception e) {
                    errors.increment();
                } finally {
                    done.countDown();
                }
            }, "ins-" + t);
            threads.add(th);
            th.start();
        }
        Thread.sleep(WARMUP.toMillis());
        Bench.Gc gc0 = Bench.Gc.now();
        measuring.set(true);
        long t0 = System.nanoTime();
        Map<String, Integer> waits = new TreeMap<>();
        int samples = 0;
        try (Connection mon = ds.getConnection(); var ps = mon.prepareStatement("""
                SELECT coalesce(wait_event_type, 'CPU') || '/' || coalesce(wait_event, '-'), count(*)
                  FROM pg_stat_activity
                 WHERE backend_type = 'client backend' AND state = 'active' AND pid <> pg_backend_pid()
                 GROUP BY 1
                """)) {
            while (System.nanoTime() - t0 < MEASURE.toNanos()) {
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        waits.merge(rs.getString(1), rs.getInt(2), Integer::sum);
                    }
                }
                samples++;
                Thread.sleep(100);
            }
        }
        measuring.set(false);
        long elapsed = System.nanoTime() - t0;
        stop.set(true);
        done.await();
        Bench.Gc gc = Bench.Gc.now().minus(gc0);
        int total = 0;
        for (int n : latN) {
            total += n;
        }
        long[] all = new long[total];
        int k = 0;
        for (int t = 0; t < conns; t++) {
            System.arraycopy(lat[t], 0, all, k, latN[t]);
            k += latN[t];
        }
        Bench.Summary s = Bench.Summary.of(all);
        double perSec = ok.sum() / (elapsed / 1e9);
        String line = String.format(Locale.ROOT, "%s,%d,%s,%d,%.1f,%d,%d,%d,%s,%d,%d", mode, conns, dist, rep, perSec,
                ok.sum(), zero.sum(), errors.sum(), s.csv(), gc.count(), gc.millis());
        System.out.println("RESULT " + line + " waits(" + samples + " samples)=" + waits);
        if (rep > 0) {
            Bench.append(summary, "mode,connections,distribution,rep,inserts_per_sec,inserted,zero_rows,errors,"
                    + Bench.Summary.HEADER + ",gc_count,gc_ms", line);
            for (var e : waits.entrySet()) {
                Bench.append(waitsFile, "mode,connections,distribution,rep,samples,wait_event,active_backends_sum",
                        String.format(Locale.ROOT, "%s,%d,%s,%d,%d,%s,%d", mode, conns, dist, rep, samples,
                                e.getKey(), e.getValue()));
            }
        }
    }
}
