package lab.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lab.Containers;
import lab.lease.LeaseRepository;
import lab.store.ChatRecord;
import lab.store.CommittedReplay;
import lab.store.Q8Support;
import lab.store.Q9Support;
import lab.store.StoreFixture;
import lab.store.TxSeqWriter;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Q9. lease를 아직 유효하게 쥔 담당자 A의 트랜잭션을, 후임 없이 {@code transaction.timeout.ms} 가 지나 조정자가 중단했을 때
 * A가 받는 예외와, 그 뒤 처리 방식 세 가지의 결과.
 *
 * <pre>
 * A: lease 획득(epoch 5, TTL 60초), initTransactions, seq 100 커밋
 * A: begin, seq 101, 102 전송, flush(브로커 ack), 커밋 전에 멈춤
 * 조정자: transaction.timeout.ms(1,500ms) 뒤 만료 검사(500ms 주기)에서 A의 트랜잭션을 중단
 * A: 깨어나 commit 하거나(W1), 한 건 더 보낸다(W2)
 * 처리: (a) 같은 producer로 abort 후 계속, (b) producer를 닫고 새 producer로 재기동(init, replay, 수락), (c) lease를 놓음
 * </pre>
 *
 * <p>g, h, i는 (b)의 재기동 중에 A의 lease가 만료되어 B가 가져가는 경쟁이다. 재기동하는 새 producer의 init을 B의 init보다
 * 늦게 둔다. assertion 은 모두 실제 실행에서 관찰한 값이다(results/Q9.md).
 */
class Q9TimeoutRestartTest {

    static final int TXN_TIMEOUT_MS = 1500;
    static final String CLEANUP_INTERVAL_MS = "500";
    static final long FIRST_SEQ = 100;

    static final String ITS = InvalidTxnStateException.class.getName();
    static final String IPE = InvalidProducerEpochException.class.getName();
    static final String PFE = ProducerFencedException.class.getName();
    static final String KE = KafkaException.class.getName();

    static LabBroker broker;
    static PostgreSQLContainer pg;
    static HikariDataSource ds;
    static LeaseRepository leases;

    @BeforeAll
    static void start() throws Exception {
        broker = LabBroker.start(Map.of("KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS",
                CLEANUP_INTERVAL_MS));
        pg = Containers.postgres();
        pg.start();
        ds = Q8Support.dataSource(pg, null, 4);
        StoreFixture.applySchema(ds);
        leases = new LeaseRepository(ds);
    }

    @AfterAll
    static void stop() {
        ds.close();
        pg.stop();
        broker.close();
    }

    /** 한 시나리오의 상태. */
    static final class Ctx {
        final String id;
        final String conv;
        final String topic;
        final String txId;
        final Report r;
        final List<TxSeqWriter> writers = new ArrayList<>();
        TxSeqWriter a;
        Q9Support.Verdict verdict;
        List<ChatRecord> committed;
        List<ChatRecord> uncommitted;
        List<LabBroker.RequestLine> requests;

        Ctx(String id) {
            this.id = id;
            this.conv = "conv-q9-" + id;
            this.topic = "q9-" + id + "-log";
            this.txId = "q9-" + id;
            this.r = new Report("Q9-" + id);
        }

        void fact(String key, Object value) {
            r.line("  [" + key + "] " + value);
        }
    }

    TxSeqWriter writer(Ctx c, String name, long epoch) {
        TxSeqWriter w = new TxSeqWriter(name, broker.bootstrap(), c.topic, c.conv, c.txId, epoch,
                Map.of(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, TXN_TIMEOUT_MS));
        c.writers.add(w);
        return w;
    }

    /** 공통 앞부분: A가 seq 100을 커밋하고, 101, 102를 보낸 채 멈춘 동안 조정자가 시간 초과로 중단한다. */
    Ctx timedOut(String id, String title) throws Exception {
        Ctx c = new Ctx(id);
        broker.createTopic(c.topic);
        leases.create(c.conv, 4);
        c.r.section("Q9 " + id + ": " + title);
        c.fact("설정", "A transaction.timeout.ms=" + TXN_TIMEOUT_MS
                + ", 브로커 transaction.abort.timed.out.transaction.cleanup.interval.ms=" + CLEANUP_INTERVAL_MS);
        long epochA = leases.tryAcquire(c.conv, "A", Duration.ofSeconds(60)).orElseThrow();
        c.fact("A lease 획득", "epoch=" + epochA + " TTL=60초");
        c.a = writer(c, "A", epochA);
        c.r.call("A initTransactions", c.a::initTransactions);
        c.a.startAt(FIRST_SEQ);
        c.r.call("A begin", c.a::begin);
        c.r.call("A send(100)", () -> c.a.send(1));
        c.r.call("A commit(100)", c.a::commit);
        c.r.call("A begin", c.a::begin);
        c.r.call("A send(101, 102) flush", () -> c.a.send(2));
        long t0 = System.nanoTime();
        c.fact("A 멈춤 직전 producerIdAndEpoch", c.a.producerIdAndEpoch());
        c.fact("A 멈춤 직전 클라이언트 상태", c.a.clientState());
        c.fact("멈춤 직후 조정자 상태", broker.describeTxnLine(c.txId));
        c.fact("멈춤 직후 HW, LSO", broker.endOffset(c.topic, IsolationLevel.READ_UNCOMMITTED) + ", "
                + broker.endOffset(c.topic, IsolationLevel.READ_COMMITTED));
        // 후임 없이 조정자가 스스로 중단하기를 기다린다. 이 동안 A는 아무 호출도 하지 않는다(멈춤).
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (broker.describeTxn(c.txId).state() != TransactionState.COMPLETE_ABORT && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        c.fact("조정자 중단 확인까지", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) + "ms (A의 flush 뒤부터)");
        c.fact("중단 뒤 조정자 상태", broker.describeTxnLine(c.txId));
        c.fact("중단 뒤 파티션 producer 상태", broker.describeProducersLine(c.topic));
        c.fact("중단 뒤 HW, LSO", broker.endOffset(c.topic, IsolationLevel.READ_UNCOMMITTED) + ", "
                + broker.endOffset(c.topic, IsolationLevel.READ_COMMITTED));
        c.fact("중단 뒤 A의 lease 유효", leases.isOwner(c.conv, epochA));
        c.r.line("  -- A 깨어남 --");
        return c;
    }

    /** 처리 방식 (b)의 기동 순서: 새 producer의 initTransactions(), read_committed replay, 마지막 순번 다음부터 수락. */
    Report.Outcome initAndReplay(Ctx c, TxSeqWriter w) {
        Report.Outcome init = c.r.call(w.name() + " initTransactions", w::initTransactions);
        c.fact(w.name() + " producerIdAndEpoch", w.producerIdAndEpoch());
        if (init.ok()) {
            CommittedReplay.Result rp = CommittedReplay.replay(broker.bootstrap(), c.topic, w.name() + "-replay@" + c.id,
                    Duration.ofSeconds(20));
            c.fact(w.name() + " replay(read_committed)", "lastSeq=" + rp.lastSeq() + " endOffset(=LSO)=" + rp.endOffset()
                    + " 읽은 레코드=" + rp.records());
            w.startAt(rp.lastSeq() + 1);
        }
        return init;
    }

    /** 트랜잭션 하나로 n건을 수락한다. 호출마다 결과를 남기고, 실패한 호출에서 멈춘다. 돌려주는 값은 실패한 호출(없으면 null). */
    Report.Outcome accept(Ctx c, TxSeqWriter w, int n) {
        Report.Outcome o = c.r.call(w.name() + " begin", w::begin);
        if (!o.ok()) {
            return o;
        }
        long from = w.nextSeq();
        o = c.r.call(w.name() + " send(" + from + (n > 1 ? ".." + (from + n - 1) : "") + ")", () -> w.send(n));
        if (!o.ok()) {
            c.fact(w.name() + " 클라이언트 상태", w.clientState());
            return o;
        }
        o = c.r.call(w.name() + " commit", w::commit);
        c.fact(w.name() + " 클라이언트 상태", w.clientState());
        return o.ok() ? null : o;
    }

    void finish(Ctx c) throws Exception {
        for (TxSeqWriter w : c.writers) {
            try {
                w.close();
            } catch (Exception e) {
                c.r.line("  " + w.name() + " close -> " + e);
            }
        }
        broker.awaitStable(c.topic);
        c.fact("마지막 조정자 상태", broker.describeTxnLine(c.txId));
        c.fact("마지막 lease", "epoch=" + leases.currentEpoch(c.conv));
        c.committed = CommittedReplay.readAll(broker.bootstrap(), c.topic, Duration.ofSeconds(20));
        c.uncommitted = broker.readAll(c.topic, IsolationLevel.READ_UNCOMMITTED).stream().map(ChatRecord::from).toList();
        c.r.line("  -- read_committed 로그");
        c.committed.forEach(x -> c.r.line("     " + Q9Support.line(x)));
        c.r.line("  -- read_uncommitted 로그");
        c.uncommitted.forEach(x -> c.r.line("     " + Q9Support.line(x)));
        for (TxSeqWriter w : c.writers) {
            c.r.line("  " + w.name() + " 브로커 ack=" + Q9Support.bodies(w.brokerAcked()) + " 성공 응답="
                    + Q9Support.bodies(w.ackedToUser()) + " 실패 응답=" + Q9Support.bodies(w.failedToUser()));
        }
        c.verdict = Q9Support.judge(c.committed, FIRST_SEQ, c.writers);
        c.r.line("  [판정] " + c.verdict.render());
        c.r.line("  kafka-dump-log 요약:");
        LabBroker.summarizeDump(broker.dumpLog(c.topic)).forEach(l -> c.r.line("    " + l));
        c.requests = broker.requestLines(FencingScenario.TXN_APIS, "\"" + c.txId + "\"", "\"" + c.topic + "\"");
        c.r.line("  브로커 요청 로그 (" + FencingScenario.TXN_APIS + "):");
        c.requests.forEach(l -> c.r.line("    " + l.render()));
        c.r.line("  브로커 로그 (" + c.txId + " 또는 " + c.topic + " 포함):");
        broker.brokerLines(l -> l.contains(c.txId) || l.contains(c.topic + "-0"))
                .forEach(l -> c.r.line("    " + l));
        c.r.save();
    }

    /** 요청 로그에서 이 시나리오의 api 응답 에러 코드(요청 순서). clientId 접두어로 거른다. */
    static List<List<Integer>> errors(Ctx c, String api, String clientPrefix) {
        return c.requests.stream().filter(l -> l.api().equals(api) && l.clientId().startsWith(clientPrefix))
                .map(LabBroker.RequestLine::errors).toList();
    }

    // ------------------------------------------------------------------ W1: 깨어나 곧바로 commit

    @Test
    void a_commitAfterTimeout_restart() throws Exception {
        Ctx c = timedOut("a-commit-restart", "W1 깨어나 commit, (b) 새 producer로 재기동");
        Report.Outcome commit = c.r.call("A commitTransaction", c.a::commit);
        c.fact("A 클라이언트 상태", c.a.clientState());
        // (a)를 시도: 같은 producer로 abort
        Report.Outcome abort = c.r.call("A abortTransaction (같은 producer로 계속하려는 시도)", c.a::abort);
        Report.Outcome begin = c.r.call("A beginTransaction", c.a::begin);
        c.r.call("A close", c.a::close);
        boolean owned = leases.isOwner(c.conv, c.a.epoch());
        c.fact("A lease 확인", owned);
        TxSeqWriter a2 = writer(c, "A2", c.a.epoch());
        Report.Outcome init = initAndReplay(c, a2);
        Report.Outcome acc = accept(c, a2, 2);
        finish(c);

        assertThat(commit.exceptionClass()).isEqualTo(ITS);
        assertThat(abort.exceptionClass()).isEqualTo(KE);
        assertThat(begin.exceptionClass()).isEqualTo(KE);
        assertThat(owned).isTrue();
        assertThat(init.ok()).isTrue();
        assertThat(acc).isNull();
        assertThat(errors(c, "END_TXN", "A@")).containsExactly(List.of(0), List.of(48));
        assertThat(Q9Support.bodies(c.a.failedToUser())).containsExactly("A-101#2", "A-102#3");
        assertThat(c.committed.stream().map(ChatRecord::body).toList()).containsExactly("A-100#1", "A2-101#1", "A2-102#2");
        assertThat(c.verdict.duplicateSeqs()).isEmpty();
        assertThat(c.verdict.gaps()).isEmpty();
        assertThat(c.verdict.ackedLost()).isEmpty();
        assertThat(c.verdict.failedStored()).isEmpty();
        assertThat(Q9Support.bodies(c.verdict.brokerAckedAborted())).containsExactly("A-101#2", "A-102#3");
    }

    @Test
    void b_commitAfterTimeout_releaseLease() throws Exception {
        Ctx c = timedOut("b-commit-release", "W1 깨어나 commit, (c) lease를 놓음");
        Report.Outcome commit = c.r.call("A commitTransaction", c.a::commit);
        c.r.call("A close", c.a::close);
        boolean released = leases.release(c.conv, c.a.epoch());
        c.fact("A lease 놓음", released);
        long epochB = leases.tryAcquire(c.conv, "B", Duration.ofSeconds(60)).orElseThrow();
        c.fact("B lease 획득", "epoch=" + epochB);
        TxSeqWriter b = writer(c, "B", epochB);
        Report.Outcome init = c.r.call("B initTransactions", b::initTransactions);
        c.r.call("B 교체 표시 커밋", b::writeMarker);
        CommittedReplay.Result rp = CommittedReplay.replay(broker.bootstrap(), c.topic, "B-replay@" + c.id,
                Duration.ofSeconds(20));
        c.fact("B replay(read_committed)", "lastSeq=" + rp.lastSeq() + " endOffset(=LSO)=" + rp.endOffset());
        b.startAt(rp.lastSeq() + 1);
        Report.Outcome acc = accept(c, b, 2);
        finish(c);

        assertThat(commit.exceptionClass()).isEqualTo(ITS);
        assertThat(released).isTrue();
        assertThat(init.ok()).isTrue();
        assertThat(acc).isNull();
        assertThat(c.committed.stream().filter(x -> !x.marker()).map(ChatRecord::body).toList())
                .containsExactly("A-100#1", "B-101#1", "B-102#2");
        assertThat(c.verdict.duplicateSeqs()).isEmpty();
        assertThat(c.verdict.gaps()).isEmpty();
        assertThat(c.verdict.ackedLost()).isEmpty();
    }

    // ------------------------------------------------------------------ W2: 깨어나 한 건 더 보냄

    Ctx sendAfterTimeout(String id, String title) throws Exception {
        Ctx c = timedOut(id, title);
        c.r.call("A send(103)", () -> c.a.send(1));
        c.fact("A 클라이언트 상태", c.a.clientState());
        c.r.call("A commitTransaction", c.a::commit);
        c.fact("A 클라이언트 상태", c.a.clientState());
        return c;
    }

    @Test
    void c_sendAfterTimeout_abortContinue_keepSeq() throws Exception {
        Ctx c = sendAfterTimeout("c-send-abort-keep", "W2 깨어나 send, (a) 같은 producer로 abort 후 계속(순번 그대로)");
        Report.Outcome abort = c.r.call("A abortTransaction", c.a::abort);
        c.fact("A 클라이언트 상태", c.a.clientState());
        c.fact("A producerIdAndEpoch", c.a.producerIdAndEpoch());
        c.fact("A 다음 순번", c.a.nextSeq());
        Report.Outcome acc = accept(c, c.a, 2);
        finish(c);

        assertThat(abort.ok()).isTrue();
        assertThat(acc).isNull();
        assertThat(errors(c, "END_TXN", "A@")).containsExactly(List.of(0), List.of(0), List.of(0));
        assertThat(c.verdict.gaps()).containsExactly(101L, 102L, 103L);
        assertThat(c.verdict.duplicateSeqs()).isEmpty();
        assertThat(c.verdict.ackedLost()).isEmpty();
        assertThat(c.verdict.failedStored()).isEmpty();
    }

    @Test
    void d_sendAfterTimeout_abortContinue_rewind() throws Exception {
        Ctx c = sendAfterTimeout("d-send-abort-rewind", "W2 깨어나 send, (a) 같은 producer로 abort 후 계속(순번 되감기)");
        long first = 101;
        Report.Outcome abort = c.r.call("A abortTransaction", c.a::abort);
        c.fact("A 클라이언트 상태", c.a.clientState());
        c.a.rewindTo(first);
        c.fact("A 다음 순번(되감음)", c.a.nextSeq());
        Report.Outcome acc = accept(c, c.a, 2);
        finish(c);

        assertThat(abort.ok()).isTrue();
        assertThat(acc).isNull();
        assertThat(c.committed.stream().map(ChatRecord::body).toList()).containsExactly("A-100#1", "A-101#5", "A-102#6");
        assertThat(c.verdict.gaps()).isEmpty();
        assertThat(c.verdict.duplicateSeqs()).isEmpty();
        assertThat(c.verdict.ackedLost()).isEmpty();
        assertThat(c.verdict.failedStored()).isEmpty();
    }

    @Test
    void e_sendAfterTimeout_restart() throws Exception {
        Ctx c = sendAfterTimeout("e-send-restart", "W2 깨어나 send, (b) 새 producer로 재기동");
        c.r.call("A close", c.a::close);
        boolean owned = leases.isOwner(c.conv, c.a.epoch());
        c.fact("A lease 확인", owned);
        TxSeqWriter a2 = writer(c, "A2", c.a.epoch());
        Report.Outcome init = initAndReplay(c, a2);
        Report.Outcome acc = accept(c, a2, 2);
        finish(c);

        assertThat(owned).isTrue();
        assertThat(init.ok()).isTrue();
        assertThat(acc).isNull();
        assertThat(c.committed.stream().map(ChatRecord::body).toList()).containsExactly("A-100#1", "A2-101#1", "A2-102#2");
        assertThat(c.verdict.gaps()).isEmpty();
        assertThat(c.verdict.duplicateSeqs()).isEmpty();
        assertThat(c.verdict.ackedLost()).isEmpty();
    }

    // ------------------------------------------------------------------ 경쟁: 재기동 중에 lease가 만료되어 B가 가져감

    /**
     * W1 뒤 (b)를 고르고, 새 producer A2를 만든 뒤 init 전에 멈춘다. 그동안 A의 lease가 만료되고 B가 lease 획득, init,
     * 교체 표시, replay, 2건 수락을 끝낸다. 그 뒤 A2가 init 한다.
     */
    record Race(Ctx c, TxSeqWriter a2, TxSeqWriter b, boolean ownedAtCheck, Report.Outcome a2Init) {
    }

    Race raceUntilA2Init(String id, String title) throws Exception {
        Ctx c = timedOut(id, title);
        c.r.call("A commitTransaction", c.a::commit);
        c.r.call("A close", c.a::close);
        boolean owned = leases.isOwner(c.conv, c.a.epoch());
        c.fact("A lease 확인(재기동 결정)", owned);
        TxSeqWriter a2 = writer(c, "A2", c.a.epoch());
        c.r.line("  -- A2 생성, init 전에 멈춤(재기동이 늦어짐) --");
        leases.forceExpire(c.conv);
        long epochB = leases.tryAcquire(c.conv, "B", Duration.ofSeconds(60)).orElseThrow();
        c.fact("A lease 만료, B lease 획득", "epoch=" + epochB);
        TxSeqWriter b = writer(c, "B", epochB);
        c.r.call("B initTransactions", b::initTransactions);
        c.fact("B producerIdAndEpoch", b.producerIdAndEpoch());
        c.r.call("B 교체 표시 커밋", b::writeMarker);
        CommittedReplay.Result rp = CommittedReplay.replay(broker.bootstrap(), c.topic, "B-replay@" + c.id,
                Duration.ofSeconds(20));
        c.fact("B replay(read_committed)", "lastSeq=" + rp.lastSeq());
        b.startAt(rp.lastSeq() + 1);
        accept(c, b, 2);
        c.r.line("  -- A2 깨어나 init --");
        Report.Outcome a2Init = c.r.call("A2 initTransactions", a2::initTransactions);
        c.fact("A2 producerIdAndEpoch", a2.producerIdAndEpoch());
        c.fact("A2 init 뒤 조정자 상태", broker.describeTxnLine(c.txId));
        return new Race(c, a2, b, owned, a2Init);
    }

    void a2ReplayAndAccept(Ctx c, TxSeqWriter a2) {
        CommittedReplay.Result rp = CommittedReplay.replay(broker.bootstrap(), c.topic, "A2-replay@" + c.id,
                Duration.ofSeconds(20));
        c.fact("A2 replay(read_committed)", "lastSeq=" + rp.lastSeq());
        a2.startAt(rp.lastSeq() + 1);
    }

    /** 처음 가설의 처리 그대로: InvalidProducerEpochException이면 abort, ProducerFencedException이면 닫고 수락을 멈춘다. */
    Report.Outcome bNextTxnPerHypothesis(Ctx c, TxSeqWriter b) {
        Report.Outcome o = accept(c, b, 1);
        if (o != null && IPE.equals(o.exceptionClass())) {
            o = c.r.call("B abortTransaction", b::abort);
            c.fact("B 클라이언트 상태", b.clientState());
        }
        return o;
    }

    @Test
    void f_restartRacesTakeover_noLeaseCheckAfterInit() throws Exception {
        Race x = raceUntilA2Init("f-race-nocheck", "경쟁, A2는 init 뒤 lease를 다시 확인하지 않음");
        Ctx c = x.c();
        a2ReplayAndAccept(c, x.a2());
        Report.Outcome a2Acc = accept(c, x.a2(), 2);
        c.r.line("  -- B의 다음 트랜잭션 --");
        Report.Outcome bOut = bNextTxnPerHypothesis(c, x.b());
        boolean bOwner = leases.isOwner(c.conv, x.b().epoch());
        c.fact("B lease 유효(처음 가설의 처리는 확인하지 않고 멈춤)", bOwner);
        c.r.line("  -- A2의 다음 트랜잭션 --");
        Report.Outcome a2Acc2 = accept(c, x.a2(), 1);
        boolean a2Owner = leases.isOwner(c.conv, x.a2().epoch());
        c.fact("A2 lease 유효(A2는 확인하지 않음)", a2Owner);
        finish(c);

        assertThat(x.ownedAtCheck()).isTrue();
        assertThat(x.a2Init().ok()).isTrue();
        assertThat(a2Acc).isNull();
        assertThat(bOut.exceptionClass()).isEqualTo(PFE);
        assertThat(bOwner).isTrue();
        assertThat(a2Acc2).isNull();
        assertThat(a2Owner).isFalse();
        assertThat(c.verdict.duplicateSeqs()).isEmpty();
        assertThat(c.verdict.gaps()).isEmpty();
        assertThat(c.verdict.ackedLost()).isEmpty();
        assertThat(c.verdict.failedStored()).isEmpty();
        assertThat(Q9Support.bodies(c.verdict.logOrderRejectedAcked())).hasSize(3);
        // A2가 epoch를 올려 데이터를 쓴 뒤라, B의 produce는 파티션 리더에서 바로 47로 거부된다(조정자에게 묻지 않음).
        List<List<Integer>> bProduce = errors(c, "PRODUCE", "B@");
        assertThat(bProduce.get(bProduce.size() - 1)).containsExactly(47);
        assertThat(errors(c, "ADD_PARTITIONS_TO_TXN", "AddPartitionsManager")).doesNotContain(List.of(0, 90));
        assertThat(errors(c, "END_TXN", "B@")).last().isEqualTo(List.of(90));
    }

    @Test
    void g_restartRacesTakeover_checkAfterInit_ownerStops() throws Exception {
        Race x = raceUntilA2Init("g-race-check-ownerstops", "경쟁, A2는 init 뒤 lease를 확인하고 멈춤, B는 처음 가설의 처리");
        Ctx c = x.c();
        boolean a2Owner = leases.isOwner(c.conv, x.a2().epoch());
        c.fact("A2 init 뒤 lease 확인", a2Owner);
        c.r.call("A2 close (담당 아님)", x.a2()::close);
        c.r.line("  -- B의 다음 트랜잭션 --");
        Report.Outcome bOut = bNextTxnPerHypothesis(c, x.b());
        boolean bOwner = leases.isOwner(c.conv, x.b().epoch());
        c.fact("B lease 유효(처음 가설의 처리는 확인하지 않고 멈춤)", bOwner);
        finish(c);

        assertThat(a2Owner).isFalse();
        assertThat(bOut.exceptionClass()).isEqualTo(PFE);
        assertThat(bOwner).isTrue();
        assertThat(c.verdict.duplicateSeqs()).isEmpty();
        assertThat(c.verdict.gaps()).isEmpty();
        assertThat(c.verdict.ackedLost()).isEmpty();
        assertThat(c.verdict.logOrderRejectedAcked()).isEmpty();
        // A2는 아무것도 쓰지 않았으므로 리더는 더 높은 epoch를 본 적이 없다. 조정자에게 묻고(90) 47로 바꿔 돌려준다.
        assertThat(errors(c, "ADD_PARTITIONS_TO_TXN", "AddPartitionsManager")).contains(List.of(0, 90));
        List<List<Integer>> bProduce = errors(c, "PRODUCE", "B@");
        assertThat(bProduce.get(bProduce.size() - 1)).containsExactly(47);
    }

    @Test
    void h_restartRacesTakeover_checkAfterInit_ownerRestarts() throws Exception {
        Race x = raceUntilA2Init("h-race-check-ownerrestarts",
                "경쟁, A2는 init 뒤 lease를 확인하고 멈춤, B는 막히면 lease를 확인해 재기동");
        Ctx c = x.c();
        boolean a2Owner = leases.isOwner(c.conv, x.a2().epoch());
        c.fact("A2 init 뒤 lease 확인", a2Owner);
        c.r.call("A2 close (담당 아님)", x.a2()::close);
        c.r.line("  -- B의 다음 트랜잭션 --");
        Report.Outcome bOut = bNextTxnPerHypothesis(c, x.b());
        c.r.call("B close", x.b()::close);
        boolean bOwner = leases.isOwner(c.conv, x.b().epoch());
        c.fact("B lease 확인", bOwner);
        TxSeqWriter b2 = writer(c, "B2", x.b().epoch());
        Report.Outcome init = initAndReplay(c, b2);
        Report.Outcome acc = accept(c, b2, 2);
        finish(c);

        assertThat(a2Owner).isFalse();
        assertThat(bOut.exceptionClass()).isEqualTo(PFE);
        assertThat(bOwner).isTrue();
        assertThat(init.ok()).isTrue();
        assertThat(acc).isNull();
        assertThat(c.verdict.duplicateSeqs()).isEmpty();
        assertThat(c.verdict.gaps()).isEmpty();
        assertThat(c.verdict.ackedLost()).isEmpty();
        assertThat(c.verdict.logOrderRejectedAcked()).isEmpty();
    }
}
