package lab.bench;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
 * Q6 (3)의 어긋남 추적. fsync=on 측정에서 첫 반복만 처리량이 30~40% 낮았다.
 * 새 컨테이너에서 같은 조건(PLAIN, 연결 8개, hot)을 8초씩 연달아 돌리며, 구간마다 WAL 파일 수,
 * 체크포인트 횟수, WAL 기록량과 sync 횟수를 함께 남긴다. 처리량이 구간에 따라 달라지면 무엇이 같이 변하는지 본다.
 */
@Tag("benchmark")
class Q6InsertWarmupDiagnostics {

    @Test
    void fsyncOnSuccessiveRuns() throws Exception {
        try (PostgreSQLContainer pg = Containers.postgres()) {
            pg.setCommand("postgres", "-c", "fsync=on");
            pg.start();
            try (HikariDataSource ds = StoreFixture.dataSource(pg, 10)) {
                StoreFixture.applySchema(ds);
                try (Connection c = ds.getConnection(); var st = c.createStatement()) {
                    st.execute("INSERT INTO conversation_owner VALUES ('c0', 1, 'owner', now() + interval '1 hour')");
                }
                AtomicLong seq = new AtomicLong();
                for (int run = 1; run <= 10; run++) {
                    String before = walStats(ds);
                    double perSec = run(ds, seq, 8);
                    String after = walStats(ds);
                    String line = String.format(Locale.ROOT, "%d,%.1f,%s,%s", run, perSec, before, after);
                    System.out.println("RESULT " + line);
                    Bench.append("q6_3_insert_warmup_diag.csv",
                            "run,inserts_per_sec,before_wal_files,before_checkpoints,before_wal_mb,before_wal_sync,"
                                    + "after_wal_files,after_checkpoints,after_wal_mb,after_wal_sync", line);
                }
            }
        }
    }

    static String walStats(HikariDataSource ds) throws Exception {
        try (Connection c = ds.getConnection(); var st = c.createStatement();
             var rs = st.executeQuery("""
                     SELECT (SELECT count(*) FROM pg_ls_waldir()),
                            (SELECT checkpoints_timed + checkpoints_req FROM pg_stat_bgwriter),
                            (SELECT round(wal_bytes / 1048576.0, 1) FROM pg_stat_wal),
                            (SELECT wal_sync FROM pg_stat_wal)
                     """)) {
            rs.next();
            return rs.getLong(1) + "," + rs.getLong(2) + "," + rs.getBigDecimal(3) + "," + rs.getLong(4);
        }
    }

    static double run(HikariDataSource ds, AtomicLong seq, int conns) throws Exception {
        AtomicBoolean stop = new AtomicBoolean();
        LongAdder ok = new LongAdder();
        List<Thread> ts = new ArrayList<>();
        for (int i = 0; i < conns; i++) {
            Thread t = new Thread(() -> {
                try (Connection c = ds.getConnection()) {
                    while (!stop.get()) {
                        ok.add(MessageStore.insert(c, FenceMode.PLAIN,
                                ChatRecord.message("c0", seq.incrementAndGet(), 1, "body-0123456789-0123456789")));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            ts.add(t);
            t.start();
        }
        long t0 = System.nanoTime();
        Thread.sleep(8000);
        stop.set(true);
        for (Thread t : ts) {
            t.join();
        }
        return ok.sum() / ((System.nanoTime() - t0) / 1e9);
    }
}
