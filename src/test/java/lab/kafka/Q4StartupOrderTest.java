package lab.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lab.shard.SeqReplay;
import lab.shard.ShardProducer;
import lab.zombie.ZombieGate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Q4. 새 담당자의 기동 순서. replay 를 initTransactions() 보다 먼저 하면 seq가 겹치는가.
 *
 * <p>A는 seq 100을 커밋한 뒤 새 트랜잭션에서 seq 101, 102를 보내고 커밋 직전에 멈춘다.
 */
class Q4StartupOrderTest {

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

    /** A: seq 100 커밋, 새 트랜잭션에서 101, 102 전송, flush, 커밋 직전에 멈춤. 깨어나면 커밋한다. */
    private static CompletableFuture<Report.Outcome> startOldOwner(Report r, ShardProducer a, ZombieGate gate) {
        return CompletableFuture.supplyAsync(() -> {
            r.call("A initTransactions", () -> a.producer().initTransactions());
            r.call("A begin", () -> a.producer().beginTransaction());
            r.sendResult("A send(100)", a.send(100), 30);
            r.call("A commit(100)", () -> a.producer().commitTransaction());
            r.call("A begin", () -> a.producer().beginTransaction());
            r.sendResult("A send(101)", a.send(101), 30);
            r.sendResult("A send(102)", a.send(102), 30);
            a.producer().flush();
            r.line("  A 커밋 직전에 멈춤. producerIdAndEpoch=" + a.producerIdAndEpoch());
            gate.pauseHere();
            r.line("  -- A 깨어남 --");
            return r.call("A commitTransaction (101, 102)", () -> a.producer().commitTransaction());
        });
    }

    private static List<String> committedValues(String topic) throws Exception {
        broker.awaitStable(topic);
        return broker.readAll(topic, IsolationLevel.READ_COMMITTED).stream().map(ConsumerRecord::value).toList();
    }

    private static void writeFrom(Report r, ShardProducer b, long firstSeq, int count) {
        r.call("B begin", () -> b.producer().beginTransaction());
        for (long s = firstSeq; s < firstSeq + count; s++) {
            r.sendResult("B send(" + s + ")", b.send(s), 30);
        }
        r.call("B commit", () -> b.producer().commitTransaction());
    }

    private static void logState(Report r, String topic, String txnId, String label) throws Exception {
        r.line("  [" + label + "] HW=" + broker.endOffset(topic, IsolationLevel.READ_UNCOMMITTED)
                + " LSO=" + broker.endOffset(topic, IsolationLevel.READ_COMMITTED)
                + " 조정자: " + broker.describeTxnLine(txnId));
    }

    private static void tail(Report r, String topic, String txnId) throws Exception {
        r.line("  read_committed 결과: " + committedValues(topic));
        r.line("  read_uncommitted 결과: " + broker.readAll(topic, IsolationLevel.READ_UNCOMMITTED).stream()
                .map(c -> c.offset() + ":" + c.value()).toList());
        r.line("  kafka-dump-log 요약:");
        LabBroker.summarizeDump(broker.dumpLog(topic)).forEach(l -> r.line("    " + l));
        r.line("  브로커 요청 로그:");
        broker.requestLines(FencingScenario.TXN_APIS, "\"" + txnId + "\"", "\"" + topic + "\"")
                .forEach(l -> r.line("    " + l.render()));
    }

    @Test
    void replayBeforeInitDuplicatesSeq() throws Exception {
        Report r = new Report("Q4-wrong-order");
        String topic = "q4-wrong-log";
        String txnId = "q4-wrong";
        broker.createTopic(topic);
        r.section("잘못된 순서: lease 획득, replay, (A 커밋), initTransactions, 수락");
        ShardProducer a = ShardProducer.transactional(broker.bootstrap(), "A", topic, txnId,
                Map.of(ProducerConfig.CLIENT_ID_CONFIG, "A@q4-wrong"));
        ZombieGate gate = new ZombieGate(Duration.ofSeconds(120));
        CompletableFuture<Report.Outcome> aCommit = startOldOwner(r, a, gate);
        gate.awaitPaused();
        logState(r, topic, txnId, "A 멈춤");

        r.line("  -- B replay (read_committed, 끝 = 시작 시점 endOffsets) --");
        SeqReplay.Result replay = SeqReplay.replay(broker.bootstrap(), topic, "B-replay@q4-wrong", Duration.ofSeconds(10));
        r.line("  B replay: lastSeq=" + replay.lastSeq() + " endOffset(=LSO)=" + replay.endOffset()
                + " position=" + replay.finalPosition() + " seen=" + replay.seen());

        r.line("  -- LSO 정체 관찰: read_committed 소비자가 HW까지 가려고 3초 poll --");
        long hw = broker.endOffset(topic, IsolationLevel.READ_UNCOMMITTED);
        long stalledPosition = pollTowards(topic, hw, Duration.ofSeconds(3), r);
        r.line("  목표 HW=" + hw + " 3초 뒤 position=" + stalledPosition);

        r.line("  -- A를 깨워 커밋시킨다 (B의 initTransactions 전) --");
        gate.resume();
        Report.Outcome aResult = aCommit.get(60, TimeUnit.SECONDS);
        logState(r, topic, txnId, "A 커밋 뒤");

        ShardProducer b = ShardProducer.transactional(broker.bootstrap(), "B", topic, txnId,
                Map.of(ProducerConfig.CLIENT_ID_CONFIG, "B@q4-wrong"));
        r.call("B initTransactions", () -> b.producer().initTransactions());
        r.line("  B producerIdAndEpoch=" + b.producerIdAndEpoch());
        writeFrom(r, b, replay.lastSeq() + 1, 2);
        a.close();
        b.close();
        tail(r, topic, txnId);
        r.save();

        assertThat(replay.lastSeq()).isEqualTo(100);
        assertThat(replay.endOffset()).isEqualTo(2); // A의 열린 트랜잭션 첫 offset(2)에서 LSO가 멈춰 있다
        assertThat(hw).isEqualTo(4);
        assertThat(stalledPosition).isEqualTo(2);
        assertThat(aResult.ok()).isTrue();
        assertThat(committedValues(topic)).containsExactly("seq=100 writer=A", "seq=101 writer=A", "seq=102 writer=A",
                "seq=101 writer=B", "seq=102 writer=B");
    }

    @Test
    void initBeforeReplayFencesOldCommit() throws Exception {
        Report r = new Report("Q4-right-order");
        String topic = "q4-right-log";
        String txnId = "q4-right";
        broker.createTopic(topic);
        r.section("올바른 순서: lease 획득, initTransactions, replay, 수락 (A는 그 뒤 깨어나 커밋 시도)");
        ShardProducer a = ShardProducer.transactional(broker.bootstrap(), "A", topic, txnId,
                Map.of(ProducerConfig.CLIENT_ID_CONFIG, "A@q4-right"));
        ZombieGate gate = new ZombieGate(Duration.ofSeconds(120));
        CompletableFuture<Report.Outcome> aCommit = startOldOwner(r, a, gate);
        gate.awaitPaused();
        logState(r, topic, txnId, "A 멈춤");

        ShardProducer b = ShardProducer.transactional(broker.bootstrap(), "B", topic, txnId,
                Map.of(ProducerConfig.CLIENT_ID_CONFIG, "B@q4-right"));
        Report.Outcome bInit = r.call("B initTransactions", () -> b.producer().initTransactions());
        r.line("  B producerIdAndEpoch=" + b.producerIdAndEpoch());
        logState(r, topic, txnId, "B init 뒤");
        SeqReplay.Result replay = SeqReplay.replay(broker.bootstrap(), topic, "B-replay@q4-right", Duration.ofSeconds(10));
        r.line("  B replay: lastSeq=" + replay.lastSeq() + " endOffset(=LSO)=" + replay.endOffset()
                + " position=" + replay.finalPosition() + " seen=" + replay.seen());

        gate.resume();
        Report.Outcome aResult = aCommit.get(60, TimeUnit.SECONDS);
        writeFrom(r, b, replay.lastSeq() + 1, 2);
        a.close();
        b.close();
        tail(r, topic, txnId);
        r.save();

        assertThat(bInit.ok()).isTrue();
        assertThat(replay.lastSeq()).isEqualTo(100);
        assertThat(replay.endOffset()).isEqualTo(5); // 100, COMMIT, 101, 102, ABORT 뒤
        assertThat(aResult.exceptionClass()).isEqualTo(ProducerFencedException.class.getName());
        assertThat(committedValues(topic)).containsExactly("seq=100 writer=A", "seq=101 writer=B", "seq=102 writer=B");
    }

    /**
     * 올바른 순서여도 A가 lease 만료 뒤, B의 initTransactions 전에 커밋을 끝내는 경우(브로커 fencing이 막지 못하는 반례).
     * 브로커는 이 커밋을 막지 못한다. B가 init 뒤에 replay 하므로 A의 커밋을 보고 그 다음 seq부터 매긴다.
     */
    @Test
    void oldCommitBeforeInitIsSeenByReplayAfterInit() throws Exception {
        Report r = new Report("Q4-commit-before-init");
        String topic = "q4-race-log";
        String txnId = "q4-race";
        broker.createTopic(topic);
        r.section("올바른 순서, 그러나 A가 B의 initTransactions 전에 커밋을 끝냄");
        ShardProducer a = ShardProducer.transactional(broker.bootstrap(), "A", topic, txnId,
                Map.of(ProducerConfig.CLIENT_ID_CONFIG, "A@q4-race"));
        ZombieGate gate = new ZombieGate(Duration.ofSeconds(120));
        CompletableFuture<Report.Outcome> aCommit = startOldOwner(r, a, gate);
        gate.awaitPaused();
        r.line("  (lease 만료, B가 lease 획득했다고 가정) A가 먼저 깨어나 커밋");
        gate.resume();
        Report.Outcome aResult = aCommit.get(60, TimeUnit.SECONDS);

        ShardProducer b = ShardProducer.transactional(broker.bootstrap(), "B", topic, txnId,
                Map.of(ProducerConfig.CLIENT_ID_CONFIG, "B@q4-race"));
        r.call("B initTransactions", () -> b.producer().initTransactions());
        SeqReplay.Result replay = SeqReplay.replay(broker.bootstrap(), topic, "B-replay@q4-race", Duration.ofSeconds(10));
        r.line("  B replay: lastSeq=" + replay.lastSeq() + " endOffset(=LSO)=" + replay.endOffset() + " seen=" + replay.seen());
        writeFrom(r, b, replay.lastSeq() + 1, 2);
        a.close();
        b.close();
        tail(r, topic, txnId);
        r.save();

        assertThat(aResult.ok()).isTrue();
        assertThat(replay.lastSeq()).isEqualTo(102);
        assertThat(committedValues(topic)).containsExactly("seq=100 writer=A", "seq=101 writer=A", "seq=102 writer=A",
                "seq=103 writer=B", "seq=104 writer=B");
    }

    private static long pollTowards(String topic, long target, Duration d, Report r) {
        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrap());
        c.put(ConsumerConfig.CLIENT_ID_CONFIG, "stall-probe@" + topic);
        c.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        TopicPartition tp = new TopicPartition(topic, 0);
        List<String> got = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(c)) {
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            long deadline = System.nanoTime() + d.toNanos();
            while (consumer.position(tp) < target && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(x -> got.add(x.offset() + ":" + x.value()));
            }
            r.line("  정체 관찰 소비자가 받은 레코드: " + got + " endOffsets()=" + consumer.endOffsets(List.of(tp)).get(tp));
            return consumer.position(tp);
        }
    }
}
