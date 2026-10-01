package lab.bench;

import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lab.Containers;
import lab.lease.LeaseRepository;
import lab.store.FenceMode;
import lab.store.StoreFixture;
import lab.store.TakeoverRace;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Q7 (2). FOR SHARE에서 A의 저장 수가 줄어드는 이유를 나눈다. Q5RaceSoakBenchmark와 같은 경쟁이지만 W의 문장을
 * {@link TakeoverRace#insertWithSnapshot} 으로 바꿔, 같은 문장 안에서 잠그지 않는 읽기로 "문장 스냅숏의 기준 epoch"를 함께 받는다.
 *
 * <p>A가 거부된 경우를 둘로 나눈다. 스냅숏이 이미 새 epoch(6)였으면 B의 교체가 W의 문장 시작 전에 커밋된 것이고,
 * 어느 방식이든 거부한다. 스냅숏이 옛 epoch(5)였는데 거부됐으면 W의 문장이 교체 커밋 전에 시작됐는데도 잠금 읽기가
 * 교체를 기다리거나 새 행 버전을 다시 읽어 거부한 것이다. LEASE_EQ라면 이 경우 넣는다.
 */
@Tag("benchmark")
class Q7ForShareRejectDiagnostics {

    static final int ITERATIONS = 20_000;
    static final String FILE = "q7_2_forshare_reject.csv";
    static final List<FenceMode> MODES = List.of(FenceMode.LEASE_EQ, FenceMode.LEASE_EQ_FOR_SHARE,
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
        Bench.env("Q7-2 " + label);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (HikariDataSource ds = StoreFixture.dataSource(pg, 4);
             PrintWriter raw = Bench.gzipWriter("q7_2_forshare_reject_raw_" + label + ".csv.gz",
                     "mode,iteration,a_inserted,a_snapshot_epoch,b_read,b_inserted,a_us,b_us")) {
            StoreFixture.applySchema(ds);
            LeaseRepository leases = new LeaseRepository(ds);
            for (FenceMode mode : MODES) {
                int stored = 0, rejSnapOld = 0, rejSnapNew = 0, storedSnapNew = 0, anomalies = 0, bDropped = 0;
                long[] aUsStored = new long[ITERATIONS], aUsRejOld = new long[ITERATIONS], aUsRejNew = new long[ITERATIONS];
                int nS = 0, nO = 0, nN = 0;
                try (Connection wc = ds.getConnection(); Connection bc = ds.getConnection()) {
                    for (int i = 0; i < ITERATIONS; i++) {
                        String id = "diag-" + mode + "-" + i;
                        TakeoverRace.prepare(ds, leases, wc, id, mode);
                        var r = TakeoverRace.run(pool, wc, bc, id, mode, true);
                        long aUs = r.aNanos() / 1000;
                        if (r.aInserted() == 1) {
                            stored++;
                            aUsStored[nS++] = aUs;
                            if (r.aSnapshotEpoch() == 6) {
                                storedSnapNew++;
                            }
                        } else if (r.aSnapshotEpoch() == 5) {
                            rejSnapOld++;
                            aUsRejOld[nO++] = aUs;
                        } else {
                            rejSnapNew++;
                            aUsRejNew[nN++] = aUs;
                        }
                        if (r.anomaly()) {
                            anomalies++;
                        }
                        if (r.bInserted() == 0) {
                            bDropped++;
                        }
                        raw.printf(Locale.ROOT, "%s,%d,%d,%d,%d,%d,%d,%d%n", mode, i, r.aInserted(), r.aSnapshotEpoch(),
                                r.bRead(), r.bInserted(), aUs, r.bNanos() / 1000);
                    }
                }
                String line = String.format(Locale.ROOT, "%s,%s,%d,%d,%d,%d,%d,%d,%d,%.3f,%.3f,%.3f", label, mode,
                        ITERATIONS, stored, rejSnapOld, rejSnapNew, storedSnapNew, anomalies, bDropped,
                        medianMs(aUsStored, nS), medianMs(aUsRejOld, nO), medianMs(aUsRejNew, nN));
                System.out.println("RESULT " + line);
                Bench.append(FILE, "postgres,mode,iterations,a_stored,a_rejected_snapshot_old,a_rejected_snapshot_new,"
                        + "a_stored_snapshot_new,anomalies,b_dropped,a_ms_p50_stored,a_ms_p50_rejected_snapshot_old,"
                        + "a_ms_p50_rejected_snapshot_new", line);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * 계측 문장에서 FOR SHARE의 A 저장 수가 원래 문장보다 훨씬 적었다. 계측이 W의 잠금 시점을 늦췄다는 가설을 보려고,
     * 원래 문장(계측 없음)의 W 출발만 0~400µs 늦춰 A 저장 수가 출발 시각 차이에 얼마나 민감한지 잰다. fsync=off, 2,000번씩.
     */
    @Test
    void delaySensitivity() throws Exception {
        try (PostgreSQLContainer pg = Containers.postgres()) {
            pg.start();
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try (HikariDataSource ds = StoreFixture.dataSource(pg, 4)) {
                StoreFixture.applySchema(ds);
                LeaseRepository leases = new LeaseRepository(ds);
                for (int rep = 1; rep <= 2; rep++) {
                    for (boolean instrumented : new boolean[] {false, true}) {
                        for (FenceMode mode : List.of(FenceMode.LEASE_EQ, FenceMode.LEASE_EQ_FOR_SHARE)) {
                            for (long delayUs : instrumented ? new long[] {0} : new long[] {0, 50, 100, 200, 400}) {
                                int stored = 0;
                                try (Connection wc = ds.getConnection(); Connection bc = ds.getConnection()) {
                                    for (int i = 0; i < 2000; i++) {
                                        String id = "sens-" + rep + "-" + instrumented + "-" + mode + "-" + delayUs + "-" + i;
                                        TakeoverRace.prepare(ds, leases, wc, id, mode);
                                        stored += TakeoverRace.run(pool, wc, bc, id, mode, instrumented, delayUs * 1000)
                                                .aInserted();
                                    }
                                }
                                String line = String.format(Locale.ROOT, "fsync-off,%d,%s,%s,%d,2000,%d", rep,
                                        instrumented ? "instrumented" : "plain", mode, delayUs, stored);
                                System.out.println("RESULT " + line);
                                Bench.append("q7_2_forshare_delay_sensitivity.csv",
                                        "postgres,rep,statement,mode,a_delay_us,iterations,a_stored", line);
                            }
                        }
                    }
                }
            } finally {
                pool.shutdownNow();
            }
        }
    }

    static double medianMs(long[] us, int n) {
        if (n == 0) {
            return Double.NaN;
        }
        long[] s = Arrays.copyOf(us, n);
        Arrays.sort(s);
        return s[(int) Math.ceil(0.5 * n) - 1] / 1000.0;
    }
}
