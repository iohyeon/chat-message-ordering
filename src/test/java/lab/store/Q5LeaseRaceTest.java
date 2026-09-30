package lab.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lab.Containers;
import lab.lease.LeaseRepository;
import lab.store.MessageStore.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Q5 (c). lease 교체와 조건부 INSERT가 겹칠 때의 경쟁. PostgreSQL 기본 격리 수준 READ COMMITTED.
 *
 * <p>문제가 되는 순서는 이렇다. 저장 워커(W)가 A의 레코드(epoch 5, seq 101)를 넣는 문장의 스냅숏에서는 lease가 5다.
 * W의 커밋이 끝나기 전에 B가 lease를 6으로 바꾸고 저장소의 마지막 seq를 읽으면, W의 삽입이 아직 보이지 않아 100을 본다.
 * B는 101을 매기고, W가 커밋한 뒤 B의 101은 ON CONFLICT DO NOTHING으로 버려진다.
 *
 * <p>Kafka 없이 JDBC 연결 두 개로 이 순서를 직접 만든다. W의 "커밋 전" 구간은 명시적 트랜잭션(배치 저장)으로 벌린다.
 */
@Testcontainers
class Q5LeaseRaceTest {

    @Container
    static final PostgreSQLContainer POSTGRES = Containers.postgres();

    static HikariDataSource ds;
    static LeaseRepository leases;
    static MessageStore store;
    final ExecutorService pool = Executors.newCachedThreadPool();
    String conv;
    Report r;
    static int counter;

    @BeforeAll
    static void setUp() throws Exception {
        ds = StoreFixture.dataSource(POSTGRES, 8);
        StoreFixture.applySchema(ds);
        leases = new LeaseRepository(ds);
        store = new MessageStore(ds);
    }

    @AfterAll
    static void tearDown() {
        ds.close();
    }

    @BeforeEach
    void each(TestInfo info) throws Exception {
        r = new Report("Q5-" + info.getTestMethod().orElseThrow().getName());
        conv = "race-" + (++counter);
        // A가 epoch 5로 lease를 쥐고 있다가 만료된 상태. seq 100까지 저장되어 있다.
        leases.create(conv, 4);
        leases.tryAcquire(conv, "A", Duration.ofSeconds(30)).orElseThrow();
        leases.forceExpire(conv);
        StoreFixture.seed(ds, conv, 100, 4);
        r.line("=== " + info.getDisplayName() + " conv=" + conv);
    }

    @AfterEach
    void after() throws Exception {
        pool.shutdownNow();
        r.save();
    }

    ChatRecord a(long seq) {
        return ChatRecord.message(conv, seq, 5, "A-" + seq);
    }

    ChatRecord b(long seq) {
        return ChatRecord.message(conv, seq, 6, "B-" + seq);
    }

    /** 작업이 주어진 시간 안에 끝나지 않으면(막혀 있으면) true. */
    static boolean blocked(Future<?> f, long millis) throws Exception {
        try {
            f.get(millis, TimeUnit.MILLISECONDS);
            return false;
        } catch (TimeoutException e) {
            return true;
        }
    }

    static int pid(Connection c) throws SQLException {
        try (var rs = c.createStatement().executeQuery("SELECT pg_backend_pid()")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    /** 다른 연결에서 본, 막힌 백엔드가 기다리는 대상. */
    static String waitOf(int pid) throws SQLException {
        try (Connection c = ds.getConnection();
             var ps = c.prepareStatement(
                     "SELECT wait_event_type, wait_event, left(query, 60) FROM pg_stat_activity WHERE pid = ?")) {
            ps.setInt(1, pid);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1) + "/" + rs.getString(2) + " query=" + rs.getString(3).replaceAll("\\s+", " ");
            }
        }
    }

    <T> Future<T> async(Callable<T> c) {
        return pool.submit(c);
    }

    void printRows() throws SQLException {
        for (Row row : store.rows(conv)) {
            r.printf("   seq=%d epoch=%d body=%s%n", row.seq(), row.epoch(), row.body());
        }
    }

    /** 두 담당자가 동시에 lease를 획득하려 하면 정확히 하나만 성공하고 epoch는 1만 오른다. */
    @Test
    void lease_concurrentAcquire_exactlyOneWins() throws Exception {
        int iterations = 300;
        int bothWon = 0, noneWon = 0;
        try (Connection c1 = ds.getConnection(); Connection c2 = ds.getConnection()) {
            for (int i = 0; i < iterations; i++) {
                String id = conv + "-acq-" + i;
                leases.create(id, 0);
                var barrier = new CyclicBarrier(2);
                Future<OptionalLong> f1 = async(() -> {
                    barrier.await();
                    return LeaseRepository.tryAcquire(c1, id, "X", Duration.ofSeconds(30));
                });
                Future<OptionalLong> f2 = async(() -> {
                    barrier.await();
                    return LeaseRepository.tryAcquire(c2, id, "Y", Duration.ofSeconds(30));
                });
                var r1 = f1.get();
                var r2 = f2.get();
                if (r1.isPresent() && r2.isPresent()) {
                    bothWon++;
                }
                if (r1.isEmpty() && r2.isEmpty()) {
                    noneWon++;
                }
                assertThat(leases.currentEpoch(id)).isEqualTo(1);
            }
        }
        r.printf("동시 획득 %d회: 둘 다 성공 %d, 둘 다 실패 %d%n", iterations, bothWon, noneWon);
        assertThat(bothWon).isZero();
        assertThat(noneWon).isZero();
    }

    /**
     * (c) PLAN의 조건(LEASE_EQ). W가 A-101을 넣고 커밋 전인 동안 B의 lease UPDATE는 막히지 않고,
     * B는 마지막 seq를 100으로 읽는다. B의 101은 W의 미커밋 행 때문에 기다렸다가 W 커밋 뒤 버려진다.
     */
    @Test
    void c1_leaseEq_openWorkerTx_newOwnerLosesSeq() throws Exception {
        try (Connection w = ds.getConnection(); Connection bConn = ds.getConnection()) {
            int bPid = pid(bConn);
            w.setAutoCommit(false);
            int n = MessageStore.insert(w, FenceMode.LEASE_EQ, a(101));
            r.line("W: A-101 삽입 행 수=" + n + " (아직 커밋 전)");

            long t0 = System.nanoTime();
            var acq = async(() -> LeaseRepository.tryAcquire(bConn, conv, "B", Duration.ofSeconds(30)));
            boolean acquireBlocked = blocked(acq, 1000);
            r.printf("B: lease UPDATE 막힘=%s, 걸린 시간=%.1fms, epoch=%s%n", acquireBlocked,
                    (System.nanoTime() - t0) / 1e6, acquireBlocked ? "-" : acq.get());
            long last = MessageStore.maxSeq(bConn, conv);
            r.line("B: 저장소에서 본 마지막 seq=" + last);

            ChatRecord bRec = b(last + 1);
            var bIns = async(() -> MessageStore.insert(bConn, FenceMode.LEASE_EQ, bRec));
            boolean insertBlocked = blocked(bIns, 1000);
            r.line("B: " + bRec.body() + " 삽입 막힘=" + insertBlocked + " 대기=" + waitOf(bPid));
            w.commit();
            int bn = bIns.get();
            r.line("W 커밋 뒤 B 삽입 행 수=" + bn + " 판정=" + MessageStore.classify(bConn, FenceMode.LEASE_EQ, bRec, bn));
            printRows();

            assertThat(n).isEqualTo(1);
            assertThat(acquireBlocked).isFalse();
            assertThat(acq.get()).hasValue(6);
            assertThat(last).isEqualTo(100);
            assertThat(insertBlocked).isTrue();
            assertThat(bn).isZero();
            assertThat(store.rows(conv)).extracting(Row::body).containsExactly("seed-100", "A-101");
        }
    }

    /** ON CONFLICT DO NOTHING은 같은 키의 미커밋 삽입을 기다린다. 그 트랜잭션이 롤백되면 기다린 쪽이 들어간다. */
    @Test
    void c1b_onConflict_waitsThenInsertsAfterRollback() throws Exception {
        try (Connection w = ds.getConnection(); Connection other = ds.getConnection()) {
            int otherPid = pid(other);
            w.setAutoCommit(false);
            MessageStore.insert(w, FenceMode.PLAIN, a(101));
            var f = async(() -> MessageStore.insert(other, FenceMode.PLAIN, b(101)));
            boolean wasBlocked = blocked(f, 1000);
            String wait = waitOf(otherPid);
            w.rollback();
            int n = f.get();
            r.line("막힘=" + wasBlocked + " 대기=" + wait + " 롤백 뒤 삽입 행 수=" + n);
            assertThat(wasBlocked).isTrue();
            assertThat(n).isEqualTo(1);
        }
    }

    /**
     * (c) FOR SHARE. W가 A-101을 넣으며 lease 행에 공유 잠금을 쥐므로, B의 lease UPDATE는 W 커밋까지 기다린다.
     * B는 W의 삽입을 본 뒤에 마지막 seq를 읽어 102부터 매긴다.
     */
    @Test
    void c2_forShare_openWorkerTx_blocksLeaseUpdate() throws Exception {
        try (Connection w = ds.getConnection(); Connection bConn = ds.getConnection()) {
            int bPid = pid(bConn);
            w.setAutoCommit(false);
            int n = MessageStore.insert(w, FenceMode.LEASE_EQ_FOR_SHARE, a(101));
            var acq = async(() -> LeaseRepository.tryAcquire(bConn, conv, "B", Duration.ofSeconds(30)));
            boolean acquireBlocked = blocked(acq, 1000);
            r.line("B: lease UPDATE 막힘=" + acquireBlocked + " 대기=" + waitOf(bPid));
            w.commit();
            var epoch = acq.get();
            long last = MessageStore.maxSeq(bConn, conv);
            ChatRecord bRec = b(last + 1);
            int bn = MessageStore.insert(bConn, FenceMode.LEASE_EQ_FOR_SHARE, bRec);
            r.line("W 커밋 뒤 B epoch=" + epoch + " 마지막 seq=" + last + " " + bRec.body() + " 삽입=" + bn);
            printRows();

            assertThat(n).isEqualTo(1);
            assertThat(acquireBlocked).isTrue();
            assertThat(epoch).hasValue(6);
            assertThat(last).isEqualTo(101);
            assertThat(bn).isEqualTo(1);
            assertThat(store.rows(conv)).extracting(Row::body).containsExactly("seed-100", "A-101", "B-102");
        }
    }

    /**
     * (c) 반대 순서. B의 lease UPDATE가 커밋 전일 때 W가 A-101을 넣는다.
     * LEASE_EQ는 기다리지 않고 커밋된 옛 epoch(5)를 보고 넣는다. FOR SHARE는 B 커밋까지 기다린 뒤 새 행(6)을 다시 읽어 거부한다.
     */
    @Test
    void c3_openLeaseUpdate_leaseEqInserts_forShareWaitsAndRejects() throws Exception {
        for (FenceMode mode : List.of(FenceMode.LEASE_EQ, FenceMode.LEASE_EQ_FOR_SHARE)) {
            String id = conv + "-" + mode;
            leases.create(id, 4);
            leases.tryAcquire(id, "A", Duration.ofSeconds(30)).orElseThrow();
            leases.forceExpire(id);
            try (Connection bConn = ds.getConnection(); Connection w = ds.getConnection()) {
                int wPid = pid(w);
                bConn.setAutoCommit(false);
                var epoch = LeaseRepository.tryAcquire(bConn, id, "B", Duration.ofSeconds(30));
                ChatRecord rec = ChatRecord.message(id, 101, 5, "A-101");
                var f = async(() -> MessageStore.insert(w, mode, rec));
                boolean wasBlocked = blocked(f, 1000);
                String wait = wasBlocked ? waitOf(wPid) : "-";
                bConn.commit();
                int n = f.get();
                r.printf("%s: B epoch=%s(커밋 전), W 막힘=%s 대기=%s, B 커밋 뒤 W 삽입 행 수=%d%n",
                        mode, epoch, wasBlocked, wait, n);
                if (mode == FenceMode.LEASE_EQ) {
                    assertThat(wasBlocked).isFalse();
                    assertThat(n).isEqualTo(1);
                } else {
                    assertThat(wasBlocked).isTrue();
                    assertThat(n).isZero();
                }
            }
        }
    }

    /**
     * (c) SERIALIZABLE. W와 B 모두 SERIALIZABLE로 두고 c1과 같은 순서를 만든다.
     * B는 lease 획득과 마지막 seq 읽기를 한 트랜잭션에서 한다. 둘 다 커밋에 성공하는지 기록한다.
     */
    @Test
    void c4_serializable_sameInterleaving() throws Exception {
        try (Connection w = ds.getConnection(); Connection bConn = ds.getConnection()) {
            w.setAutoCommit(false);
            w.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            bConn.setAutoCommit(false);
            bConn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            List<String> log = new ArrayList<>();
            int n = MessageStore.insert(w, FenceMode.LEASE_EQ, a(101));
            log.add("W insert A-101 -> " + n);
            var acq = async(() -> LeaseRepository.tryAcquire(bConn, conv, "B", Duration.ofSeconds(30)));
            boolean acquireBlocked = blocked(acq, 1000);
            log.add("B acquire blocked=" + acquireBlocked + " epoch=" + acq.get());
            long last = MessageStore.maxSeq(bConn, conv);
            log.add("B maxSeq=" + last);
            String bCommit, wCommit;
            try {
                bConn.commit();
                bCommit = "ok";
            } catch (SQLException e) {
                bCommit = e.getSQLState() + " " + e.getMessage().lines().findFirst().orElse("");
                bConn.rollback();
            }
            log.add("B commit -> " + bCommit);
            try {
                w.commit();
                wCommit = "ok";
            } catch (SQLException e) {
                wCommit = e.getSQLState() + " " + e.getMessage().lines().findFirst().orElse("");
                w.rollback();
            }
            log.add("W commit -> " + wCommit);
            log.forEach(s -> r.line("   " + s));
            r.line("   lease epoch=" + leases.currentEpoch(conv));
            printRows();
            // 둘 다 커밋되면 c1의 어긋남(B가 100을 보고도 A-101이 저장됨)이 그대로 남는다.
            assertThat(bCommit.equals("ok") && wCommit.equals("ok")).isFalse();
        }
    }

    /**
     * (c) 명시적 트랜잭션 없이 autocommit 문장끼리 경쟁시킨다. W는 A의 seq 1(epoch 5)을 넣고,
     * B는 lease를 6으로 바꾼 뒤 마지막 seq를 읽고 그다음 seq에 넣는다. B가 0을 읽었는데 A의 1이 들어가 있으면 어긋남이다.
     * slowMs가 0보다 크면 epoch 5 행 삽입에 AFTER 트리거로 지연을 넣어 "문장 스냅숏과 커밋 사이" 구간을 넓힌다.
     */
    @Test
    void c5_autocommitRace_counts() throws Exception {
        int iterations = 2000;
        List<String> summary = new ArrayList<>();
        for (int slowMs : new int[] {0, 5}) {
            setSlowTrigger(slowMs);
            for (FenceMode mode : List.of(FenceMode.LEASE_EQ, FenceMode.LEASE_EQ_FOR_SHARE)) {
                int anomalies = 0, aStored = 0, aRejected = 0;
                try (Connection wc = ds.getConnection(); Connection bc = ds.getConnection()) {
                    for (int i = 0; i < iterations; i++) {
                        String id = "r" + slowMs + "-" + mode + "-" + i;
                        leases.create(id, 4);
                        LeaseRepository.tryAcquire(wc, id, "A", Duration.ofSeconds(30)).orElseThrow();
                        leases.forceExpire(id);
                        var barrier = new CyclicBarrier(2);
                        Future<Integer> fa = async(() -> {
                            barrier.await();
                            return MessageStore.insert(wc, mode, ChatRecord.message(id, 1, 5, "A-1"));
                        });
                        Future<long[]> fb = async(() -> {
                            barrier.await();
                            long e = LeaseRepository.tryAcquire(bc, id, "B", Duration.ofSeconds(30)).orElseThrow();
                            long m = MessageStore.maxSeq(bc, id);
                            int bn = MessageStore.insert(bc, mode, ChatRecord.message(id, m + 1, e, "B-" + (m + 1)));
                            return new long[] {m, bn};
                        });
                        int an = fa.get();
                        long[] br = fb.get();
                        if (an == 1) {
                            aStored++;
                        } else {
                            aRejected++;
                        }
                        if (an == 1 && br[0] == 0) {
                            anomalies++;
                            assertThat(br[1]).isZero();
                        }
                    }
                }
                String line = String.format("slowMs=%d mode=%s 반복=%d A저장=%d A거부=%d 어긋남(B가 A의 삽입을 못 보고 같은 seq를 잃음)=%d",
                        slowMs, mode, iterations, aStored, aRejected, anomalies);
                summary.add(line);
                r.line(line);
                if (mode == FenceMode.LEASE_EQ_FOR_SHARE) {
                    assertThat(anomalies).isZero();
                }
            }
        }
        setSlowTrigger(0);
        summary.forEach(s -> r.line("RESULT " + s));
    }

    static void setSlowTrigger(int ms) throws SQLException {
        try (Connection c = ds.getConnection(); var st = c.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS slow_old_epoch ON message");
            if (ms > 0) {
                st.execute("""
                        CREATE OR REPLACE FUNCTION slow_old_epoch() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN PERFORM pg_sleep(%s); RETURN NULL; END $$
                        """.formatted(ms / 1000.0));
                st.execute("""
                        CREATE TRIGGER slow_old_epoch AFTER INSERT ON message
                        FOR EACH ROW WHEN (NEW.epoch = 5) EXECUTE FUNCTION slow_old_epoch()
                        """);
            }
        }
    }
}
