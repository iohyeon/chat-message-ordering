package lab.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import lab.kafka.FencingScenario.Prior;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Q2. 새 producer의 initTransactions() 뒤 옛 producer의 호출은 어디서 무슨 예외로 거부되는가.
 *
 * <p>assertion 은 모두 실제 실행에서 관찰한 값이다(results/Q2.md). 에러 코드: 47 INVALID_PRODUCER_EPOCH,
 * 51 CONCURRENT_TRANSACTIONS, 90 PRODUCER_FENCED.
 */
class Q2TransactionalFencingTest {

    static final String IPE = InvalidProducerEpochException.class.getName();
    static final String PFE = ProducerFencedException.class.getName();
    static final String OK = null;

    static LabBroker broker;
    static ClientLogCapture clientLogs;

    @BeforeAll
    static void start() {
        broker = LabBroker.start();
        clientLogs = new ClientLogCapture();
    }

    @AfterAll
    static void stop() {
        clientLogs.close();
        broker.close();
    }

    private FencingScenario run(FencingScenario s) throws Exception {
        Report r = new Report("Q2-" + s.id);
        s.run(broker, clientLogs, r);
        r.save();
        // 공통: A는 seq 100 커밋에서 epoch 0 -> 1 로 올라간다(TV2는 트랜잭션이 끝날 때마다 epoch를 올린다).
        assertThat(s.facts.get("A 초기 producerIdAndEpoch")).endsWith("epoch=0)");
        assertThat(s.facts.get("A seq 100 커밋 뒤 producerIdAndEpoch")).endsWith("epoch=1)");
        // 거부된 A의 레코드는 로그에 없다. 로그에 남은 A의 레코드는 seq 100과, 멈추기 전에 보낸 seq 101뿐이다.
        // (후임이 없는 e-timeout 은 A가 복구해 다시 쓰므로 제외한다.)
        if (s.successor) {
            List<String> aPayloads = LabBroker.summarizeDump(s.dump).stream().filter(l -> l.contains("writer=A")).toList();
            assertThat(aPayloads).hasSize(s.prior == Prior.SENT ? 2 : 1);
        }
        return s;
    }

    private static void assertOps(FencingScenario s, String... expected) {
        for (int i = 0; i < expected.length; i++) {
            assertThat(s.op(i).exceptionClass()).as("op#%d %s", i, s.ops.get(i)).isEqualTo(expected[i]);
        }
    }

    private static void assertSendCallback(FencingScenario s, int i, String expected) {
        assertThat(s.opCallback(i).exceptionClass()).as("op#%d callback", i).isEqualTo(expected);
    }

    @Test
    void a_none_sendFirst() throws Exception {
        FencingScenario s = run(new FencingScenario("a-none", Prior.NONE, List.of("send", "commit", "abort")));
        assertOps(s, IPE, IPE, PFE);
        assertSendCallback(s, 0, IPE);
        assertThat(s.facts.get("B producerIdAndEpoch")).endsWith("epoch=2)");
        // 파티션이 더 높은 epoch를 본 적이 없으므로 브로커가 조정자에게 AddPartitionsToTxn 으로 묻고,
        // 조정자의 90(PRODUCER_FENCED)을 produce 응답에서는 47로 바꿔 돌려준다.
        assertThat(s.errorsOf("ADD_PARTITIONS_TO_TXN", "AddPartitionsManager-client-1")).contains(List.of(0, 90));
        assertThat(s.errorsOf("PRODUCE", "A@a-none")).containsExactly(List.of(0), List.of(47));
        assertThat(s.errorsOf("END_TXN", "A@a-none")).containsExactly(List.of(0), List.of(90));
    }

    @Test
    void a_sent_sendFirst() throws Exception {
        FencingScenario s = run(new FencingScenario("a-sent", Prior.SENT, List.of("send", "commit", "abort")));
        assertOps(s, IPE, IPE, PFE);
        assertSendCallback(s, 0, IPE);
        // 열린 트랜잭션을 중단하는 abort 마커가 epoch 2로 쓰이고, B는 epoch 3을 받는다.
        assertThat(s.facts.get("B producerIdAndEpoch")).endsWith("epoch=3)");
        assertThat(s.facts.get("fencing 뒤 파티션 producer 상태")).contains("producerEpoch=2");
        // 파티션이 이미 epoch 2를 봤으므로 조정자에게 묻지 않고 리더에서 바로 47로 거부한다.
        assertThat(s.errorsOf("ADD_PARTITIONS_TO_TXN", "AddPartitionsManager-client-1")).doesNotContain(List.of(0, 90));
        assertThat(s.errorsOf("PRODUCE", "A@a-sent")).containsExactly(List.of(0), List.of(0), List.of(47));
        assertThat(s.errorsOf("INIT_PRODUCER_ID", "B@a-sent")).containsExactly(List.of(51), List.of(0));
    }

    @Test
    void b_none_commitFirst() throws Exception {
        FencingScenario s = run(new FencingScenario("b-none", Prior.NONE, List.of("commit", "begin", "send", "commit")));
        // 아무것도 보내지 않은 트랜잭션의 commit 은 EndTxn 을 보내지 않고 성공한다.
        assertOps(s, OK, OK, IPE, IPE);
        List<List<Integer>> endTxn = s.errorsOf("END_TXN", "A@b-none");
        assertThat(endTxn.get(0)).isEqualTo(List.of(0));
        assertThat(endTxn.get(endTxn.size() - 1)).isEqualTo(List.of(90)); // close() 가 보낸 abort
    }

    @Test
    void b_sent_commitFirst() throws Exception {
        FencingScenario s = run(new FencingScenario("b-sent", Prior.SENT, List.of("commit", "abort")));
        assertOps(s, PFE, PFE);
        assertThat(s.errorsOf("END_TXN", "A@b-sent")).containsExactly(List.of(0), List.of(90));
    }

    @Test
    void c_none_abortFirst() throws Exception {
        FencingScenario s = run(new FencingScenario("c-none", Prior.NONE, List.of("abort", "begin", "send")));
        assertOps(s, OK, OK, IPE);
    }

    @Test
    void c_sent_abortFirst() throws Exception {
        FencingScenario s = run(new FencingScenario("c-sent", Prior.SENT, List.of("abort", "begin", "send")));
        assertOps(s, PFE, PFE, PFE);
        assertSendCallback(s, 2, PFE);
        // 치명 오류 상태의 send() 는 요청을 보내지 않고 호출한 스레드에서 바로 콜백을 부른다.
        assertThat(s.facts.get("op#2 send callbackThread")).doesNotStartWith("kafka-producer-network-thread");
        assertThat(s.errorsOf("PRODUCE", "A@c-sent")).hasSize(2);
    }

    @Test
    void d_idle_beginFirst() throws Exception {
        FencingScenario s = run(new FencingScenario("d-idle", Prior.IDLE, List.of("begin", "send", "commit")));
        assertOps(s, OK, IPE, IPE);
        assertThat(s.errorsOf("ADD_PARTITIONS_TO_TXN", "AddPartitionsManager-client-1")).contains(List.of(0, 90));
    }

    @Test
    void e_timeout_noSuccessor() throws Exception {
        FencingScenario s = run(new FencingScenario("e-timeout", Prior.SENT, false,
                List.of("send", "commit", "abort", "begin", "send", "commit"),
                Map.of(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 3000)));
        // 후임이 없으면 A는 abort 로 조정자가 올린 epoch를 넘겨받아 다시 쓸 수 있다(KIP-588).
        assertOps(s, IPE, IPE, OK, OK, OK, OK);
        assertThat(s.facts.get("시간 초과 대기 뒤 조정자 상태")).contains("CompleteAbort").contains("producerEpoch=2");
        assertThat(s.facts.get("op#2 abort 뒤 A 클라이언트 상태")).isEqualTo("READY");
    }
}
