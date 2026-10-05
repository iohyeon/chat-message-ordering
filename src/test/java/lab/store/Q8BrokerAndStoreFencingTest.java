package lab.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lab.Containers;
import lab.lease.LeaseRepository;
import lab.store.MessageStore.Outcome;
import lab.store.Q8Support.WorkerKey;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Q8. 브로커 fencing(트랜잭션 producer, read_committed, 기동 순서 lease 획득, initTransactions, replay, 수락)에
 * 저장소 세대 검사를 더했을 때, 이전 담당자 A가 성공 응답한 레코드가 저장되는가.
 *
 * <p>A와 B는 같은 {@code transactional.id} 를 쓰는 트랜잭션 producer다. A는 seq 100을 커밋한 뒤 101, 102를 보내고
 * 커밋 전에 멈춘다. A의 커밋 시점을 세 곳에 둔다: (a) lease 교체 전, (b) lease 교체 뒤 B의 initTransactions() 전,
 * (c) B의 initTransactions() 뒤. 같은 로그를 저장 워커 8개(비교 방식 4가지와, 교체 전에 따라잡은 워커와 밀린 워커)가
 * read_committed 로 처리한다. 밀린 워커는 A의 seq 100까지만 교체 전에 처리하고, 나머지는 B의 수락이 끝난 뒤 처리한다.
 * B는 init 뒤 교체 표시를 쓴다. 표시는 LOG_ORDER 워커만 쓰고 나머지 워커와 replay는 건너뛴다.
 */
@Testcontainers
class Q8BrokerAndStoreFencingTest {

    @Container
    static final KafkaContainer KAFKA = Containers.kafka();

    @Container
    static final PostgreSQLContainer POSTGRES = Containers.postgres();

    static HikariDataSource publicDs;
    static final Map<WorkerKey, HikariDataSource> DS = new LinkedHashMap<>();
    static LeaseRepository leases;
    static Admin admin;

    @BeforeAll
    static void setUp() throws Exception {
        publicDs = Q8Support.dataSource(POSTGRES, null, 4);
        Q8Support.createSchemas(publicDs, Q8Support.keys());
        for (WorkerKey k : Q8Support.keys()) {
            DS.put(k, Q8Support.dataSource(POSTGRES, k.schema(), 2));
        }
        leases = new LeaseRepository(publicDs);
        admin = Q8Support.admin(KAFKA.getBootstrapServers());
    }

    @AfterAll
    static void tearDown() {
        DS.values().forEach(HikariDataSource::close);
        publicDs.close();
        admin.close();
    }

    enum When {
        BEFORE_LEASE("lease 교체 전"),
        AFTER_LEASE_BEFORE_INIT("lease 교체 뒤, B의 initTransactions() 전"),
        AFTER_INIT("B의 initTransactions() 뒤");

        final String label;

        When(String label) {
            this.label = label;
        }
    }

    /** 한 시나리오의 결과. */
    record Result(boolean aCommitOk, String aCommitError, long replayLastSeq, List<ChatRecord> aAcked,
                  List<ChatRecord> aFailed, List<ChatRecord> bAcked, String zombieError,
                  Map<WorkerKey, List<StoreWorker.Processed>> processed, Map<WorkerKey, Map<Long, MessageStore.Row>> rows,
                  Map<Long, List<String>> logDuplicates) {

        Outcome outcome(WorkerKey k, String body) {
            return processed.get(k).stream().filter(p -> p.record().body().equals(body)).findFirst()
                    .map(StoreWorker.Processed::outcome).orElse(null);
        }
    }

    Result run(Report r, When when) throws Exception {
        String id = UUID.randomUUID().toString().substring(0, 8);
        String topic = "q8-" + id;
        String conv = "conv-" + id;
        String txId = "tx-" + conv;
        String bootstrap = KAFKA.getBootstrapServers();
        StoreFixture.createTopic(bootstrap, topic);
        leases.create(conv, 4);
        r.line("=== A의 커밋 시점: " + when.label + " topic=" + topic + " conv=" + conv + " transactional.id=" + txId);

        Map<WorkerKey, StoreWorker> workers = new LinkedHashMap<>();
        Map<WorkerKey, List<StoreWorker.Processed>> processed = new LinkedHashMap<>();
        for (WorkerKey k : Q8Support.keys()) {
            workers.put(k, new StoreWorker(bootstrap, topic, DS.get(k), k.mode(), "read_committed"));
            processed.put(k, new ArrayList<>());
        }
        TxEpochWriter a = null;
        TxEpochWriter b = null;
        try {
            long epochA = leases.tryAcquire(conv, "A", Duration.ofSeconds(30)).orElseThrow();
            a = new TxEpochWriter("A", bootstrap, topic, conv, txId, epochA, Map.of());
            a.initTransactions();
            a.startAt(100);
            a.accept(List.of("A-100"));
            r.line("A lease epoch=" + epochA + ", initTransactions, seq 100 커밋");
            stable(r, topic, "A-100 커밋 뒤");
            drain(r, "교체 전 처리(모든 워커)", workers, processed, true);

            a.beginAndSend(List.of("A-101", "A-102"));
            r.line("A: seq 101, 102 전송, flush(브로커 ack 받음), 커밋 전에 멈춤");
            offsets(r, topic, "A 멈춤");

            boolean aOk = false;
            String aErr = null;
            if (when == When.BEFORE_LEASE) {
                String[] e = commitA(r, a);
                aOk = e == null;
                aErr = e == null ? null : e[0];
                stable(r, topic, "A 커밋 뒤");
                drain(r, "A 커밋 직후 처리(따라잡은 워커)", workers, processed, false);
            }

            leases.forceExpire(conv);
            long epochB = leases.tryAcquire(conv, "B", Duration.ofSeconds(30)).orElseThrow();
            r.line("lease 만료(A는 모름), B lease 획득 epoch=" + epochB + ", lease 테이블 epoch=" + leases.currentEpoch(conv));

            if (when == When.AFTER_LEASE_BEFORE_INIT) {
                String[] e = commitA(r, a);
                aOk = e == null;
                aErr = e == null ? null : e[0];
                stable(r, topic, "A 커밋 뒤");
                drain(r, "A 커밋 직후 처리(따라잡은 워커, B의 init 전)", workers, processed, false);
            }

            b = new TxEpochWriter("B", bootstrap, topic, conv, txId, epochB, Map.of());
            long t0 = System.nanoTime();
            b.initTransactions();
            r.line("B initTransactions -> 성공 (" + (System.nanoTime() - t0) / 1_000_000 + "ms)");

            if (when == When.AFTER_INIT) {
                String[] e = commitA(r, a);
                aOk = e == null;
                aErr = e == null ? null : e[0];
                stable(r, topic, "A 커밋 시도 뒤");
                drain(r, "A 커밋 시도 직후 처리(따라잡은 워커)", workers, processed, false);
            }

            b.writeMarker();
            r.line("B: 교체 표시(epoch " + epochB + ") 트랜잭션 커밋");
            CommittedReplay.Result replay = CommittedReplay.replay(bootstrap, topic, "B-replay-" + id, Duration.ofSeconds(20));
            r.line("B replay(read_committed): lastSeq=" + replay.lastSeq() + " endOffset(=LSO)=" + replay.endOffset()
                    + " 읽은 레코드=" + replay.records());
            b.startAt(replay.lastSeq() + 1);
            long s = replay.lastSeq() + 1;
            b.accept(List.of("B-" + s, "B-" + (s + 1)));
            r.line("B: seq " + s + ", " + (s + 1) + " 커밋, 성공 응답");

            String zombieError;
            try {
                a.beginAndSend(List.of("A-zombie"));
                a.commit();
                zombieError = null;
                r.line("A 좀비 전송 -> 성공(예상 밖)");
            } catch (Exception e) {
                Throwable c = e instanceof java.util.concurrent.ExecutionException && e.getCause() != null ? e.getCause() : e;
                zombieError = c.getClass().getSimpleName();
                r.line("A 좀비 전송(B 수락 뒤) -> " + c.getClass().getName() + ": " + c.getMessage());
            }

            stable(r, topic, "끝");
            drain(r, "B 수락 뒤 처리(모든 워커)", workers, processed, true);

            List<ChatRecord> log = CommittedReplay.readAll(bootstrap, topic, Duration.ofSeconds(20));
            r.line("-- read_committed 로그");
            for (ChatRecord c : log) {
                r.line("   " + (c.marker() ? "MARKER epoch=" + c.epoch() : "seq=" + c.seq() + " epoch=" + c.epoch()
                        + " body=" + c.body()));
            }
            Map<Long, List<String>> dup = Q8Support.duplicateSeqsInLog(log);
            r.line("read_committed 로그에서 두 번 이상 나온 seq: " + dup);
            r.line("A가 성공 응답한 것: " + bodies(a.ackedToUser()) + ", 실패 응답한 것: " + bodies(a.failedToUser()));
            r.line("B가 성공 응답한 것: " + bodies(b.ackedToUser()));

            Map<WorkerKey, Map<Long, MessageStore.Row>> rows = new LinkedHashMap<>();
            r.line("-- 워커별 message 테이블");
            for (WorkerKey k : Q8Support.keys()) {
                var rs = Q8Support.rows(DS.get(k), conv);
                rows.put(k, rs);
                long slotTaken = processed.get(k).stream().filter(p -> p.outcome() == Outcome.SLOT_TAKEN).count();
                r.printf("   %-30s 저장된 seq=%s 빈자리=%s A 성공 응답 미저장=%s A 실패 응답 저장=%s B 성공 응답 미저장=%s SLOT_TAKEN=%d",
                        k.label(), rs.values().stream().map(x -> x.seq() + ":" + x.body()).toList(), Q8Support.gaps(rs),
                        bodies(Q8Support.ackedButNotStored(a.ackedToUser(), rs)),
                        bodies(Q8Support.failedButStored(a.failedToUser(), rs)),
                        bodies(Q8Support.ackedButNotStored(b.ackedToUser(), rs)), slotTaken);
            }
            return new Result(aOk, aErr, replay.lastSeq(), a.ackedToUser(), a.failedToUser(), b.ackedToUser(),
                    zombieError, processed, rows, dup);
        } finally {
            workers.values().forEach(StoreWorker::close);
            if (a != null) {
                a.close();
            }
            if (b != null) {
                b.close();
            }
        }
    }

    static List<String> bodies(List<ChatRecord> rs) {
        return rs.stream().map(ChatRecord::body).toList();
    }

    /** A의 커밋. 성공이면 null, 실패면 {예외 클래스 이름}. */
    static String[] commitA(Report r, TxEpochWriter a) {
        long t0 = System.nanoTime();
        try {
            a.commit();
            r.line("A commitTransaction (101, 102) -> 성공 (" + (System.nanoTime() - t0) / 1_000_000 + "ms), 사용자에게 성공 응답");
            return null;
        } catch (RuntimeException e) {
            r.line("A commitTransaction (101, 102) -> " + e.getClass().getName() + ": " + e.getMessage() + " ("
                    + (System.nanoTime() - t0) / 1_000_000 + "ms), 사용자에게 실패 응답");
            return new String[] {e.getClass().getName()};
        }
    }

    static void offsets(Report r, String topic, String label) throws Exception {
        r.line("  [" + label + "] HW=" + Q8Support.endOffset(admin, topic, IsolationLevel.READ_UNCOMMITTED)
                + " LSO=" + Q8Support.endOffset(admin, topic, IsolationLevel.READ_COMMITTED));
    }

    static void stable(Report r, String topic, String label) throws Exception {
        long[] o = Q8Support.awaitStable(admin, topic);
        r.line("  [" + label + "] HW=" + o[0] + " LSO=" + o[1]);
    }

    /** 워커들이 지금 LSO까지 처리하게 한다. includeLagging이 false면 따라잡은 워커만. */
    static void drain(Report r, String title, Map<WorkerKey, StoreWorker> workers,
                      Map<WorkerKey, List<StoreWorker.Processed>> processed, boolean includeLagging)
            throws SQLException {
        r.line("-- " + title);
        for (var e : workers.entrySet()) {
            WorkerKey k = e.getKey();
            if (k.lagging() && !includeLagging) {
                continue;
            }
            var ps = e.getValue().drainToEnd(Duration.ofSeconds(20));
            processed.get(k).addAll(ps);
            List<String> items = new ArrayList<>();
            for (var p : ps) {
                items.add(p.record().marker() ? "MARKER(e" + p.record().epoch() + ")"
                        : p.record().body() + "(e" + p.record().epoch() + ")->" + p.outcome());
            }
            r.printf("   %-30s %s", k.label(), items);
        }
    }

    /** 뷰에 붙인 FOR SHARE가 public.conversation_owner의 행을 잠그는지 확인한다. 이것이 거짓이면 LEASE_EQ_FOR_SHARE 비교가 무의미하다. */
    @Test
    void v_forShareThroughViewLocksLeaseRow() throws Exception {
        Report r = new Report("Q8-v_forShareThroughViewLocksLeaseRow");
        String conv = "conv-view-" + UUID.randomUUID().toString().substring(0, 8);
        leases.create(conv, 4);
        long epoch = leases.tryAcquire(conv, "A", Duration.ofMillis(1)).orElseThrow();
        Thread.sleep(5);
        WorkerKey k = new WorkerKey(FenceMode.LEASE_EQ_FOR_SHARE, false);
        try (Connection w = DS.get(k).getConnection(); Connection b = publicDs.getConnection()) {
            w.setAutoCommit(false);
            int n = MessageStore.insert(w, FenceMode.LEASE_EQ_FOR_SHARE, ChatRecord.message(conv, 1, epoch, "A-1"));
            r.line("W(스키마 " + k.schema() + "): 뷰를 통한 FOR SHARE 삽입 행 수=" + n + " (커밋 전)");
            String lockMode;
            try (var ps = b.prepareStatement("""
                    SELECT string_agg(l.mode, ',') FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
                     WHERE c.relname = 'conversation_owner' AND c.relnamespace = 'public'::regnamespace
                       AND l.pid <> pg_backend_pid()
                    """); var rs = ps.executeQuery()) {
                rs.next();
                lockMode = rs.getString(1);
            }
            r.line("public.conversation_owner 테이블 수준 잠금(다른 백엔드): " + lockMode);
            String updateResult;
            try (var st = b.createStatement()) {
                st.execute("SET lock_timeout = '500ms'");
                st.executeUpdate("UPDATE conversation_owner SET epoch = epoch + 1 WHERE conversation_id = '" + conv + "'");
                updateResult = "막히지 않음";
            } catch (SQLException e) {
                updateResult = "SQLSTATE " + e.getSQLState() + " " + e.getMessage();
            }
            r.line("B: public.conversation_owner lease UPDATE(lock_timeout 500ms) -> " + updateResult);
            w.rollback();
            r.save();
            assertThat(n).isEqualTo(1);
            assertThat(lockMode).contains("RowShareLock");
            assertThat(updateResult).startsWith("SQLSTATE 55P03");
        }
    }

    @Test
    void a_commitBeforeLeaseTakeover() throws Exception {
        Report r = new Report("Q8-a_commitBeforeLeaseTakeover");
        Result res = run(r, When.BEFORE_LEASE);
        r.save();
        assertThat(res.aCommitOk()).isTrue();
        assertThat(res.replayLastSeq()).isEqualTo(102);
        assertThat(res.logDuplicates()).isEmpty();
        for (WorkerKey k : Q8Support.keys()) {
            boolean leaseCompare = k.mode() == FenceMode.LEASE_EQ || k.mode() == FenceMode.LEASE_EQ_FOR_SHARE;
            var rows = res.rows().get(k);
            assertThat(Q8Support.ackedButNotStored(res.bAcked(), rows)).as(k.label()).isEmpty();
            if (leaseCompare && k.lagging()) {
                // 교체 전에 정상 커밋했어도 밀린 워커는 lease가 바뀐 뒤 처리하므로 거부한다(Q5 (b)와 같은 원인).
                assertThat(res.outcome(k, "A-101")).as(k.label()).isEqualTo(Outcome.REJECTED_EPOCH);
                assertThat(rows.keySet()).as(k.label()).containsExactly(100L, 103L, 104L);
                assertThat(bodies(Q8Support.ackedButNotStored(res.aAcked(), rows))).containsExactly("A-101", "A-102");
            } else {
                assertThat(rows.keySet()).as(k.label()).containsExactly(100L, 101L, 102L, 103L, 104L);
                assertThat(Q8Support.ackedButNotStored(res.aAcked(), rows)).as(k.label()).isEmpty();
            }
        }
    }

    /** 처음 가설의 경우. A는 lease 교체 뒤, B의 initTransactions() 전에 커밋을 끝낸다. */
    @Test
    void b_commitAfterLeaseBeforeInit() throws Exception {
        Report r = new Report("Q8-b_commitAfterLeaseBeforeInit");
        Result res = run(r, When.AFTER_LEASE_BEFORE_INIT);
        r.save();
        assertThat(res.aCommitOk()).isTrue();
        assertThat(res.replayLastSeq()).isEqualTo(102);
        assertThat(res.logDuplicates()).isEmpty();
        assertThat(bodies(res.bAcked())).containsExactly("B-103", "B-104");
        assertThat(res.zombieError()).isNotNull();
        for (WorkerKey k : Q8Support.keys()) {
            boolean leaseCompare = k.mode() == FenceMode.LEASE_EQ || k.mode() == FenceMode.LEASE_EQ_FOR_SHARE;
            var rows = res.rows().get(k);
            assertThat(Q8Support.ackedButNotStored(res.bAcked(), rows)).as(k.label()).isEmpty();
            if (leaseCompare) {
                assertThat(res.outcome(k, "A-101")).as(k.label()).isEqualTo(Outcome.REJECTED_EPOCH);
                assertThat(res.outcome(k, "A-102")).as(k.label()).isEqualTo(Outcome.REJECTED_EPOCH);
                assertThat(rows.keySet()).as(k.label()).containsExactly(100L, 103L, 104L);
                assertThat(Q8Support.gaps(rows)).as(k.label()).containsExactly(101L, 102L);
                assertThat(bodies(Q8Support.ackedButNotStored(res.aAcked(), rows))).as(k.label())
                        .containsExactly("A-101", "A-102");
            } else {
                assertThat(rows.keySet()).as(k.label()).containsExactly(100L, 101L, 102L, 103L, 104L);
                assertThat(Q8Support.ackedButNotStored(res.aAcked(), rows)).as(k.label()).isEmpty();
            }
        }
    }

    @Test
    void c_commitAfterInitIsFenced() throws Exception {
        Report r = new Report("Q8-c_commitAfterInitIsFenced");
        Result res = run(r, When.AFTER_INIT);
        r.save();
        assertThat(res.aCommitOk()).isFalse();
        assertThat(res.aCommitError()).isEqualTo(ProducerFencedException.class.getName());
        assertThat(res.replayLastSeq()).isEqualTo(100);
        assertThat(res.logDuplicates()).isEmpty();
        assertThat(bodies(res.aFailed())).containsExactly("A-101", "A-102");
        for (WorkerKey k : Q8Support.keys()) {
            var rows = res.rows().get(k);
            assertThat(rows.values()).as(k.label()).extracting(MessageStore.Row::body)
                    .containsExactly("A-100", "B-101", "B-102");
            assertThat(Q8Support.failedButStored(res.aFailed(), rows)).as(k.label()).isEmpty();
            assertThat(Q8Support.ackedButNotStored(res.aAcked(), rows)).as(k.label()).isEmpty();
        }
    }
}
