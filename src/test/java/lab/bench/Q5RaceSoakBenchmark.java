package lab.bench;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lab.Containers;
import lab.lease.LeaseRepository;
import lab.store.ChatRecord;
import lab.store.FenceMode;
import lab.store.MessageStore;
import lab.store.StoreFixture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Q5 (c) 보충. Q5LeaseRaceTest.c5 와 같은 경쟁을 지연 트리거 없이 많이 반복해, 인위적으로 구간을 넓히지 않아도
 * 어긋남이 나오는지 센다. 일반 테스트에 넣기엔 오래 걸려 benchmark 태그로 뺐다.
 */
@Tag("benchmark")
class Q5RaceSoakBenchmark {

    static final int ITERATIONS = 20_000;
    static final String FILE = "q5_race_soak.csv";

    /** Testcontainers 기본값(fsync=off). 커밋이 WAL flush를 기다리지 않아 스냅숏과 커밋 사이가 짧다. */
    @Test
    void fsyncOff() throws Exception {
        try (PostgreSQLContainer pg = Containers.postgres()) {
            pg.start();
            soak(pg, "fsync-off");
        }
    }

    /** PostgreSQL 기본값(fsync=on). 커밋이 WAL flush를 기다리는 만큼 구간이 길어진다. */
    @Test
    void fsyncOn() throws Exception {
        try (PostgreSQLContainer pg = Containers.postgres()) {
            pg.setCommand("postgres", "-c", "fsync=on");
            pg.start();
            soak(pg, "fsync-on");
        }
    }

    void soak(PostgreSQLContainer pg, String label) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (HikariDataSource ds = StoreFixture.dataSource(pg, 4)) {
            StoreFixture.applySchema(ds);
            try (Connection c = ds.getConnection(); var rs = c.createStatement().executeQuery("SHOW fsync")) {
                rs.next();
                System.out.println("PGSETTING fsync=" + rs.getString(1) + " (" + label + ")");
            }
            LeaseRepository leases = new LeaseRepository(ds);
            for (FenceMode mode : new FenceMode[] {FenceMode.LEASE_EQ, FenceMode.LEASE_EQ_FOR_SHARE}) {
                int anomalies = 0, aStored = 0;
                long t0 = System.nanoTime();
                try (Connection wc = ds.getConnection(); Connection bc = ds.getConnection()) {
                    for (int i = 0; i < ITERATIONS; i++) {
                        String id = "soak-" + mode + "-" + i;
                        leases.create(id, 4);
                        LeaseRepository.tryAcquire(wc, id, "A", Duration.ofSeconds(30)).orElseThrow();
                        leases.forceExpire(id);
                        var barrier = new CyclicBarrier(2);
                        Future<Integer> fa = pool.submit(() -> {
                            barrier.await();
                            return MessageStore.insert(wc, mode, ChatRecord.message(id, 1, 5, "A-1"));
                        });
                        Future<Long> fb = pool.submit(() -> {
                            barrier.await();
                            LeaseRepository.tryAcquire(bc, id, "B", Duration.ofSeconds(30)).orElseThrow();
                            return MessageStore.maxSeq(bc, id);
                        });
                        int an = fa.get();
                        long m = fb.get();
                        if (an == 1) {
                            aStored++;
                            if (m == 0) {
                                anomalies++;
                            }
                        }
                    }
                }
                String line = String.format(Locale.ROOT, "%s,%s,%d,%d,%d,%.1f", label, mode, ITERATIONS, aStored, anomalies,
                        (System.nanoTime() - t0) / 1e9);
                System.out.println("RESULT " + line);
                Bench.append(FILE, "postgres,mode,iterations,a_stored,anomalies,seconds", line);
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
