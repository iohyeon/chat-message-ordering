package lab.bench;

import com.zaxxer.hikari.HikariDataSource;
import java.util.List;
import lab.Containers;
import lab.store.FenceMode;
import lab.store.StoreFixture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Q7 (4). LOG_ORDER가 한 대화에 쓰기가 몰릴 때 느려지는 원인을 나눈다. Q6 (3)과 같은 측정({@link Q6StoreInsertBenchmark#runOnce})을
 * hot 분포(모든 연결이 대화 하나에 씀)에서 fence 행을 다루는 방식만 바꿔 잰다.
 * <ul>
 *   <li>LOG_ORDER: 레코드마다 fence 행을 UPDATE(새 행 버전)하고 배타 잠금을 커밋까지 쥔다.
 *   <li>LOG_ORDER_FOR_UPDATE: 새 행 버전 없이 배타 잠금만 커밋까지 쥔다.
 *   <li>LOG_ORDER_FOR_SHARE: 공유 잠금만 쥔다. 공유 잠금끼리는 기다리지 않는다.
 *   <li>LOG_ORDER_SNAPSHOT: 잠그지 않는다(경쟁에 안전하지 않은 대조군).
 * </ul>
 */
@Tag("benchmark")
class Q7FenceWriteBenchmark {

    static final List<FenceMode> MODES = List.of(FenceMode.PLAIN, FenceMode.LOG_ORDER, FenceMode.LOG_ORDER_FOR_UPDATE,
            FenceMode.LOG_ORDER_FOR_SHARE, FenceMode.LOG_ORDER_SNAPSHOT);
    static final int REPEATS = 3;

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
        Bench.env("Q7-4 " + label);
        var bench = new Q6StoreInsertBenchmark();
        bench.summary = "q7_4_fence_write_" + label + ".csv";
        bench.waitsFile = "q7_4_fence_write_waits_" + label + ".csv";
        Bench.deleteIfExists(bench.summary);
        Bench.deleteIfExists(bench.waitsFile);
        try (HikariDataSource ds = StoreFixture.dataSource(pg, 34)) {
            StoreFixture.applySchema(ds);
            Q6StoreInsertBenchmark.printSettings(ds);
            bench.runOnce(ds, FenceMode.LOG_ORDER, 8, "hot", 0);
            for (int rep = 1; rep <= REPEATS; rep++) {
                for (int conns : new int[] {1, 8, 32}) {
                    for (FenceMode mode : MODES) {
                        bench.runOnce(ds, mode, conns, "hot", rep);
                    }
                }
            }
        }
    }
}
