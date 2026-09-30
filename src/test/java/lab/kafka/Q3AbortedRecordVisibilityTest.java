package lab.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import lab.kafka.FencingScenario.Prior;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Q3. Q2(a-sent)의 로그를 read_uncommitted, read_committed 로 처음부터 읽는다.
 * abort 된 A의 seq 101이 어느 격리 수준에서 보이는지, 트랜잭션 마커가 offset을 차지하는지 확인한다.
 */
class Q3AbortedRecordVisibilityTest {

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

    @Test
    void abortedRecordsAreVisibleOnlyToReadUncommitted() throws Exception {
        Report r = new Report("Q3");
        FencingScenario s = new FencingScenario("q3", Prior.SENT, List.of("send", "commit", "abort"));
        s.run(broker, clientLogs, r);

        broker.awaitStable(s.topic);
        r.section("read_uncommitted 로 처음부터 읽기");
        LabBroker.ReadResult ru = broker.readAll(s.topic, IsolationLevel.READ_UNCOMMITTED, Duration.ofSeconds(20));
        ru.records().forEach(c -> r.line("  offset=" + c.offset() + " value=" + c.value()));
        r.line("  endOffset=" + ru.endOffset() + " 읽기를 마친 position=" + ru.finalPosition());

        r.section("read_committed 로 처음부터 읽기");
        LabBroker.ReadResult rc = broker.readAll(s.topic, IsolationLevel.READ_COMMITTED, Duration.ofSeconds(20));
        rc.records().forEach(c -> r.line("  offset=" + c.offset() + " value=" + c.value()));
        r.line("  endOffset=" + rc.endOffset() + " 읽기를 마친 position=" + rc.finalPosition());

        r.section("브로커 요청 로그: 두 소비자의 FETCH 응답 (abortedTransactions 필드)");
        List<LabBroker.RequestLine> fetches = broker.requestLines(List.of("FETCH"), "reader-read-");
        fetches.stream().filter(l -> l.clientId().endsWith("@" + s.topic) && !l.response().contains("\"recordsSizeInBytes\":0"))
                .forEach(l -> r.line("  " + l.render()));
        r.save();

        assertThat(ru.records()).extracting(ConsumerRecord::offset).containsExactly(0L, 2L, 4L);
        assertThat(ru.records()).extracting(ConsumerRecord::value)
                .containsExactly("seq=100 writer=A", "seq=101 writer=A", "seq=101 writer=B");
        assertThat(rc.records()).extracting(ConsumerRecord::offset).containsExactly(0L, 4L);
        assertThat(rc.records()).extracting(ConsumerRecord::value).containsExactly("seq=100 writer=A", "seq=101 writer=B");
        // 마커는 offset 1, 3, 5를 차지하지만 어느 소비자에게도 레코드로 돌아오지 않는다. 두 소비자 모두 마커 뒤까지 position이 간다.
        assertThat(ru.finalPosition()).isEqualTo(6);
        assertThat(rc.finalPosition()).isEqualTo(6);
        assertThat(LabBroker.summarizeDump(s.dump)).filteredOn(l -> l.contains("endTxnMarker"))
                .extracting(l -> l.replaceAll(".*offset=(\\d+).*endTxnMarker: (\\w+).*", "$1 $2"))
                .containsExactly("1 COMMIT", "3 ABORT", "5 COMMIT");
    }
}
