package lab.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lab.shard.ShardProducer;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Q8 반복 측정에서 나온 어긋남의 확인. 이전 담당자 A의 커밋이 거부될 때 예외가 두 가지였다.
 * B의 initTransactions() 가 돌아오기 전에 A가 커밋하면 InvalidTxnStateException, 돌아온 뒤면 ProducerFencedException.
 *
 * <p>B의 첫 InitProducerId는 A의 열린 트랜잭션을 abort하고 CONCURRENT_TRANSACTIONS를 받는다. 클라이언트는
 * retry.backoff.ms 뒤에 다시 보낸다. 그 사이 조정자는 CompleteAbort 상태이고 epoch는 A의 epoch + 1이다.
 * 이 사이에 A의 EndTxn(commit)이 도착하면 조정자는 이것을 "epoch를 올린 뒤의 재시도"로 보고(epoch + 1 일치),
 * CompleteAbort에 대한 commit이므로 INVALID_TXN_STATE를 돌려준다는 것이 소스를 읽고 세운 가설이다.
 */
class Q8CommitDuringInitTest {

    static LabBroker broker;

    @BeforeAll
    static void start() {
        broker = LabBroker.start();
    }

    @AfterAll
    static void stop() {
        broker.close();
    }

    @Test
    void commitBetweenAbortAndInitRetry() throws Exception {
        Report r = new Report("Q8-commit-during-init-backoff");
        String topic = "q8-itx-log";
        String txnId = "q8-itx";
        broker.createTopic(topic);
        r.section("A의 커밋이 B의 initTransactions() 안(첫 InitProducerId의 abort 뒤, 재시도 전)에 도착");
        ShardProducer a = ShardProducer.transactional(broker.bootstrap(), "A", topic, txnId,
                Map.of(ProducerConfig.CLIENT_ID_CONFIG, "A@q8-itx"));
        r.call("A initTransactions", () -> a.producer().initTransactions());
        r.call("A begin", () -> a.producer().beginTransaction());
        r.sendResult("A send(101)", a.send(101), 30);
        a.producer().flush();
        r.line("  A producerIdAndEpoch=" + a.producerIdAndEpoch() + " 조정자: " + broker.describeTxnLine(txnId));

        ShardProducer b = ShardProducer.transactional(broker.bootstrap(), "B", topic, txnId,
                Map.of(ProducerConfig.CLIENT_ID_CONFIG, "B@q8-itx"));
        long t0 = System.nanoTime();
        CompletableFuture<Report.Outcome> bInit = CompletableFuture.supplyAsync(
                () -> r.call("B initTransactions", () -> b.producer().initTransactions()));
        // 조정자가 A의 트랜잭션을 abort로 끝낼 때까지 기다린다. B의 init은 아직 돌아오지 않았어야 한다.
        TransactionDescription d;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        do {
            d = broker.describeTxn(txnId);
        } while (d.state() != TransactionState.COMPLETE_ABORT && System.nanoTime() < deadline);
        boolean bDoneBeforeCommit = bInit.isDone();
        r.line("  [A 커밋 직전] " + (System.nanoTime() - t0) / 1_000_000 + "ms 조정자: state=" + d.state()
                + " producerEpoch=" + d.producerEpoch() + " B init 끝남=" + bDoneBeforeCommit);
        Report.Outcome aCommit = r.call("A commitTransaction (101)", () -> a.producer().commitTransaction());
        Report.Outcome bResult = bInit.get(30, TimeUnit.SECONDS);
        r.line("  B producerIdAndEpoch=" + b.producerIdAndEpoch() + " 조정자: " + broker.describeTxnLine(txnId));
        Report.Outcome aAgain = r.call("A commitTransaction 다시", () -> a.producer().commitTransaction());

        r.call("B begin", () -> b.producer().beginTransaction());
        r.sendResult("B send(101)", b.send(101), 30);
        r.call("B commit", () -> b.producer().commitTransaction());
        a.close();
        b.close();
        broker.awaitStable(topic);
        r.line("  read_committed 결과: " + broker.readAll(topic, IsolationLevel.READ_COMMITTED).stream()
                .map(x -> x.value()).toList());
        r.line("  브로커 요청 로그:");
        broker.requestLines(List.of("INIT_PRODUCER_ID", "END_TXN"), "\"" + txnId + "\"")
                .forEach(l -> r.line("    " + l.render()));
        r.save();

        assertThat(bDoneBeforeCommit).isFalse();
        assertThat(d.state()).isEqualTo(TransactionState.COMPLETE_ABORT);
        assertThat(aCommit.exceptionClass()).isEqualTo(InvalidTxnStateException.class.getName());
        assertThat(bResult.ok()).isTrue();
        assertThat(broker.readAll(topic, IsolationLevel.READ_COMMITTED)).extracting(x -> x.value())
                .containsExactly("seq=101 writer=B");
        // 확인용: A가 다시 커밋을 부르면 클라이언트가 이미 fatal 상태다.
        assertThat(aAgain.ok()).isFalse();
    }
}
