package lab.store;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import javax.sql.DataSource;
import lab.lease.LeaseRepository;

/**
 * Q5 (c)와 Q7이 함께 쓰는 경쟁 한 판. 저장 워커(W)가 옛 담당자 A의 seq 1(epoch 5)을 넣는 문장과,
 * 새 담당자 B의 교체(lease UPDATE, LOG_ORDER 계열이면 fence 갱신), 마지막 seq 읽기, 다음 seq 삽입을
 * {@link CyclicBarrier} 로 동시에 출발시킨다. 모든 문장은 autocommit이다.
 *
 * <p>LOG_ORDER 계열에서 B가 fence를 직접 올리는 것은 "교체 표시를 다른 워커가 처리한 경우"를 흉내 낸다.
 * 워커 하나가 로그를 순서대로 처리하면 A의 삽입과 표시 처리는 겹치지 않으므로, 이 경쟁은 컨슈머 그룹 재배정 중
 * 옛 워커와 새 워커가 잠깐 같은 파티션을 처리하는 경우에 해당한다.
 */
public final class TakeoverRace {

    private TakeoverRace() {
    }

    /**
     * 한 판의 결과.
     *
     * @param aInserted W가 넣은 A의 행 수(0 또는 1)
     * @param aSnapshotEpoch W의 문장 스냅숏에서 본 기준 epoch(lease 또는 fence). 계측 문장을 쓴 경우만, 아니면 -1
     * @param bRead B가 교체 뒤 읽은 마지막 seq
     * @param bInserted B가 넣은 행 수. 0이면 B의 레코드가 버려진 것이다
     * @param aNanos W 문장의 걸린 시간
     * @param bNanos B의 교체부터 삽입까지 걸린 시간
     */
    public record Round(int aInserted, long aSnapshotEpoch, long bRead, int bInserted, long aNanos, long bNanos) {

        /** B가 A의 삽입을 보지 못하고 같은 seq를 매겼는데 A가 저장된 경우. */
        public boolean anomaly() {
            return aInserted == 1 && bRead == 0;
        }
    }

    /** 대화 하나를 준비한다. A가 epoch 5로 lease를 쥐었다가 만료된 상태. LOG_ORDER 계열이면 fence도 5로 둔다. */
    public static void prepare(DataSource ds, LeaseRepository leases, Connection wc, String id, FenceMode mode)
            throws SQLException {
        leases.create(id, 4);
        LeaseRepository.tryAcquire(wc, id, "A", Duration.ofSeconds(30)).orElseThrow();
        leases.forceExpire(id);
        if (mode.usesFence()) {
            try (Connection c = ds.getConnection(); var ps = c.prepareStatement(
                    "INSERT INTO message_fence (conversation_id, max_epoch) VALUES (?, 5)")) {
                ps.setString(1, id);
                ps.executeUpdate();
            }
        }
    }

    /**
     * 경쟁 한 판. {@code instrumented} 이면 W의 문장을 {@link #insertWithSnapshot} 로 바꿔 스냅숏의 기준 epoch도 받는다.
     */
    public static Round run(ExecutorService pool, Connection wc, Connection bc, String id, FenceMode mode,
            boolean instrumented) throws Exception {
        return run(pool, wc, bc, id, mode, instrumented, 0);
    }

    /** {@code aDelayNanos} 만큼 W의 문장을 늦게 보낸다(바쁜 대기). 경쟁 결과가 출발 시각 차이에 얼마나 민감한지 보려는 것이다. */
    public static Round run(ExecutorService pool, Connection wc, Connection bc, String id, FenceMode mode,
            boolean instrumented, long aDelayNanos) throws Exception {
        var barrier = new CyclicBarrier(2);
        Future<long[]> fa = pool.submit(() -> {
            barrier.await();
            long until = System.nanoTime() + aDelayNanos;
            while (System.nanoTime() < until) {
                Thread.onSpinWait();
            }
            ChatRecord rec = ChatRecord.message(id, 1, 5, "A-1");
            long t0 = System.nanoTime();
            long[] res = instrumented ? insertWithSnapshot(wc, mode, rec)
                    : new long[] {MessageStore.insert(wc, mode, rec), -1};
            return new long[] {res[0], res[1], System.nanoTime() - t0};
        });
        Future<long[]> fb = pool.submit(() -> {
            barrier.await();
            long t0 = System.nanoTime();
            long e = LeaseRepository.tryAcquire(bc, id, "B", Duration.ofSeconds(30)).orElseThrow();
            if (mode.usesFence()) {
                MessageStore.bumpFence(bc, id, e);
            }
            long m = MessageStore.maxSeq(bc, id);
            int bn = MessageStore.insert(bc, mode, ChatRecord.message(id, m + 1, e, "B-" + (m + 1)));
            return new long[] {m, bn, System.nanoTime() - t0};
        });
        long[] a = fa.get();
        long[] b = fb.get();
        return new Round((int) a[0], a[1], b[0], (int) b[1], a[2], b[2]);
    }

    /**
     * 모드의 SQL을 그대로 쓰되, 같은 문장 안에서 잠그지 않는 읽기로 기준 epoch를 함께 읽는다.
     * READ COMMITTED에서 한 문장의 CTE는 모두 같은 스냅숏을 쓰므로, 돌려주는 epoch는 "문장이 시작될 때 커밋되어 있던 값"이다.
     * 잠금 읽기가 기다린 뒤 새 행 버전을 다시 읽어도 이 값은 바뀌지 않는다. 돌려주는 값은 {삽입 행 수, 스냅숏 epoch}.
     */
    public static long[] insertWithSnapshot(Connection c, FenceMode mode, ChatRecord r) throws SQLException {
        if (mode == FenceMode.LOG_ORDER) {
            // LOG_ORDER는 SQL 자체가 데이터 변경 CTE를 가지므로 다른 CTE 안에 넣을 수 없다(PostgreSQL 제약).
            throw new IllegalArgumentException("LOG_ORDER는 계측 문장으로 감쌀 수 없다");
        }
        String snapSql = mode.usesFence()
                ? "SELECT max_epoch AS e FROM message_fence WHERE conversation_id = ?"
                : "SELECT epoch AS e FROM conversation_owner WHERE conversation_id = ?";
        String sql = "WITH snap AS (" + snapSql + "), ins AS (" + mode.sql().strip() + " RETURNING 1) "
                + "SELECT (SELECT e FROM snap), (SELECT count(*) FROM ins)";
        try (var ps = c.prepareStatement(sql)) {
            ps.setString(1, r.conversationId());
            // 모드의 자리표시자는 snap의 자리표시자 하나 뒤에 온다.
            MessageStore.bind(ps, mode, r, 1);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return new long[] {rs.getLong(2), rs.getLong(1)};
            }
        }
    }
}
