package lab.bench;

import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lab.Containers;
import lab.lease.LeaseRepository;
import lab.store.FenceMode;
import lab.store.TakeoverRace;
import lab.store.StoreFixture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Q5 (c) 보충과 Q7 (1). Q5LeaseRaceTest.c5 와 같은 경쟁({@link TakeoverRace})을 지연 트리거 없이 많이 반복한다.
 * W는 A의 seq 1(epoch 5)을 넣고, B는 교체(lease UPDATE, LOG_ORDER 계열이면 fence 갱신), 마지막 seq 읽기,
 * 그다음 seq 삽입까지 한다. "B가 0을 읽었는데 A가 저장됨"(어긋남)과 B의 삽입이 0행으로 버려진 횟수를 센다.
 * 반복이 끝나면 message 테이블을 직접 세어 "A의 행만 있고 B의 행이 없는 대화" 수로 한 번 더 확인한다.
 */
@Tag("benchmark")
class Q5RaceSoakBenchmark {

    static final int ITERATIONS = 20_000;
    static final String FILE = "q5_race_soak.csv";
    static final List<FenceMode> MODES = List.of(FenceMode.LEASE_EQ, FenceMode.LEASE_EQ_FOR_SHARE, FenceMode.LOG_ORDER,
            FenceMode.LOG_ORDER_FOR_SHARE, FenceMode.LOG_ORDER_SNAPSHOT);

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
        Bench.env("Q5 soak " + label);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (HikariDataSource ds = StoreFixture.dataSource(pg, 4);
             PrintWriter raw = Bench.gzipWriter("q5_race_soak_raw_" + label + ".csv.gz",
                     "mode,iteration,a_inserted,b_read,b_inserted,a_us,b_us")) {
            StoreFixture.applySchema(ds);
            try (Connection c = ds.getConnection(); var rs = c.createStatement().executeQuery("SHOW fsync")) {
                rs.next();
                System.out.println("PGSETTING fsync=" + rs.getString(1) + " (" + label + ")");
            }
            LeaseRepository leases = new LeaseRepository(ds);
            for (FenceMode mode : MODES) {
                int anomalies = 0, aStored = 0, bInserted = 0, bDropped = 0;
                long t0 = System.nanoTime();
                String prefix = "soak-" + mode + "-";
                try (Connection wc = ds.getConnection(); Connection bc = ds.getConnection()) {
                    for (int i = 0; i < ITERATIONS; i++) {
                        String id = prefix + i;
                        TakeoverRace.prepare(ds, leases, wc, id, mode);
                        var r = TakeoverRace.run(pool, wc, bc, id, mode, false);
                        if (r.aInserted() == 1) {
                            aStored++;
                        }
                        if (r.anomaly()) {
                            anomalies++;
                        }
                        if (r.bInserted() == 1) {
                            bInserted++;
                        } else {
                            bDropped++;
                        }
                        raw.printf(Locale.ROOT, "%s,%d,%d,%d,%d,%d,%d%n", mode, i, r.aInserted(), r.bRead(),
                                r.bInserted(), r.aNanos() / 1000, r.bNanos() / 1000);
                    }
                }
                double seconds = (System.nanoTime() - t0) / 1e9;
                long[] table = tableCounts(ds, prefix);
                String line = String.format(Locale.ROOT, "%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%.1f", label, mode, ITERATIONS,
                        aStored, ITERATIONS - aStored, anomalies, bInserted, bDropped, table[0], table[1], seconds);
                System.out.println("RESULT " + line);
                Bench.append(FILE, "postgres,mode,iterations,a_stored,a_rejected,anomalies,b_inserted,b_dropped,"
                        + "table_a_only,table_rows,seconds", line);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /** 반복이 끝난 뒤 message 테이블에서 센다. {A의 행만 있고 B의 행이 없는 대화 수, 전체 행 수}. */
    static long[] tableCounts(HikariDataSource ds, String prefix) throws Exception {
        try (Connection c = ds.getConnection(); var ps = c.prepareStatement("""
                SELECT count(*) FILTER (WHERE a_rows = 1 AND b_rows = 0), coalesce(sum(a_rows + b_rows), 0)
                  FROM (SELECT conversation_id,
                               count(*) FILTER (WHERE epoch = 5) AS a_rows,
                               count(*) FILTER (WHERE epoch = 6) AS b_rows
                          FROM message WHERE starts_with(conversation_id, ?) GROUP BY conversation_id) t
                """)) {
            ps.setString(1, prefix);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return new long[] {rs.getLong(1), rs.getLong(2)};
            }
        }
    }
}
