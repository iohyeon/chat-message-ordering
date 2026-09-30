package lab.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lab.shard.ShardProducer;
import lab.zombie.ZombieGate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Q1. 멱등 producer만 쓰면 옛 담당자의 쓰기가 막히는가.
 *
 * <p>A가 seq 101, 102를 보내고 멈춘다. B가 새 멱등 producer로 seq 101, 102를 보낸다. A가 깨어나 seq 103을 보낸다.
 */
class Q1IdempotentProducerTest {

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
    void oldIdempotentProducerIsNotBlocked() throws Exception {
        Report r = new Report("Q1");
        String topic = "q1-chat-log";
        broker.createTopic(topic);

        r.section("클라이언트 기본값 (kafka-clients 4.3.1 ConfigDef)");
        Object idempotenceDefault = ProducerConfig.configDef().defaultValues().get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
        Object acksDefault = ProducerConfig.configDef().defaultValues().get(ProducerConfig.ACKS_CONFIG);
        Object isolationDefault = ConsumerConfig.configDef().defaultValues().get(ConsumerConfig.ISOLATION_LEVEL_CONFIG);
        r.line("  enable.idempotence 기본값 = " + idempotenceDefault);
        r.line("  acks 기본값 = " + acksDefault);
        r.line("  isolation.level 기본값 = " + isolationDefault);

        ZombieGate gate = new ZombieGate(Duration.ofSeconds(60));
        List<Report.SendOutcome> aOutcomes = new ArrayList<>();
        List<String> aIds = new ArrayList<>();

        ShardProducer a = ShardProducer.idempotent(broker.bootstrap(), "A", topic);
        ShardProducer b = ShardProducer.idempotent(broker.bootstrap(), "B", topic);

        r.section("A: seq 101, 102 전송 후 멈춤");
        CompletableFuture<Void> aThread = CompletableFuture.runAsync(() -> {
            aOutcomes.add(r.sendResult("A send(101)", a.send(101), 30));
            aOutcomes.add(r.sendResult("A send(102)", a.send(102), 30));
            aIds.add(a.producerIdAndEpoch());
            gate.pauseHere();
            aOutcomes.add(r.sendResult("A send(103) 깨어난 뒤", a.send(103), 30));
            aIds.add(a.producerIdAndEpoch());
        });
        gate.awaitPaused();
        r.line("  A 클라이언트 producerIdAndEpoch = " + aIds.get(0));

        r.section("B: 새 멱등 producer로 seq 101, 102 전송");
        List<Report.SendOutcome> bOutcomes = new ArrayList<>();
        bOutcomes.add(r.sendResult("B send(101)", b.send(101), 30));
        bOutcomes.add(r.sendResult("B send(102)", b.send(102), 30));
        String bId = b.producerIdAndEpoch();
        r.line("  B 클라이언트 producerIdAndEpoch = " + bId);

        r.section("A 깨어남");
        gate.resume();
        aThread.get();
        r.line("  A 클라이언트 producerIdAndEpoch(깨어난 뒤) = " + aIds.get(1));

        r.section("파티션의 producer 상태 (Admin.describeProducers)");
        r.line("  " + broker.describeProducersLine(topic));

        broker.awaitStable(topic);
        r.section("read_uncommitted 소비자");
        List<ConsumerRecord<String, String>> ru = broker.readAll(topic, IsolationLevel.READ_UNCOMMITTED);
        ru.forEach(c -> r.line("  offset=" + c.offset() + " value=" + c.value()));
        r.section("read_committed 소비자");
        List<ConsumerRecord<String, String>> rc = broker.readAll(topic, IsolationLevel.READ_COMMITTED);
        rc.forEach(c -> r.line("  offset=" + c.offset() + " value=" + c.value()));

        r.section("kafka-dump-log 요약");
        String dump = broker.dumpLog(topic);
        List<String> summary = LabBroker.summarizeDump(dump);
        r.lines(summary);
        r.section("kafka-dump-log 원문");
        r.line(dump);

        r.section("클라이언트 로그 (producer ID 할당)");
        r.lines(clientLogs.lines(l -> l.contains("ProducerId set to") || l.contains("Instantiated an idempotent")
                || l.contains("idempotence")));

        r.section("브로커 요청 로그: InitProducerId");
        r.lines(broker.requests(List.of("INIT_PRODUCER_ID")));
        r.section("브로커 요청 로그: Produce");
        r.lines(broker.requests(List.of("PRODUCE"), "\"clientId\":\"writer-A\"", "\"clientId\":\"writer-B\""));
        r.save();

        a.close();
        b.close();

        // 관찰값으로 고정한 assertion
        assertThat(idempotenceDefault).isEqualTo(true);
        assertThat(isolationDefault).isEqualTo("read_uncommitted");
        assertThat(aOutcomes).allMatch(o -> o.future().ok() && o.callback().ok());
        assertThat(bOutcomes).allMatch(o -> o.future().ok() && o.callback().ok());
        assertThat(ru).extracting(ConsumerRecord::value).containsExactly(
                "seq=101 writer=A", "seq=102 writer=A", "seq=101 writer=B", "seq=102 writer=B", "seq=103 writer=A");
        assertThat(rc).extracting(ConsumerRecord::value).containsExactlyElementsOf(
                ru.stream().map(ConsumerRecord::value).toList());
        assertThat(summary.stream().filter(s -> s.startsWith("baseOffset")).map(s -> s.replaceAll(".*producerId=(\\S+).*", "$1"))
                .distinct().count()).isEqualTo(2);
    }

    /** transactional.id 를 주면서 enable.idempotence=false 를 명시하면 producer 생성이 실패한다. */
    @Test
    void transactionalIdRequiresIdempotence() throws Exception {
        Report r = new Report("Q1-config-transactional");
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrap(),
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "q1-config",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        Report.Outcome o = r.call("new KafkaProducer(transactional.id=q1-config, enable.idempotence=false)",
                () -> new KafkaProducer<String, String>(config).close());
        r.save();
        assertThat(o.exceptionClass()).isEqualTo(ConfigException.class.getName());
        assertThat(o.message()).contains("Cannot set a transactional.id without also enabling idempotence");
    }

    /**
     * enable.idempotence 를 설정하지 않고 acks=1 을 주면 멱등이 조용히 꺼진다. 배치에 producer ID가 붙지 않는다(-1).
     * 멱등 producer만 쓰는 설계도 acks=all 을 유지해야 멱등이라는 뜻이다.
     */
    @Test
    void acksOneSilentlyDisablesIdempotence() throws Exception {
        Report r = new Report("Q1-config-acks1");
        String topic = "q1-acks1-log";
        broker.createTopic(topic);
        clientLogs.clear();
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrap(),
                ProducerConfig.CLIENT_ID_CONFIG, "acks1",
                ProducerConfig.ACKS_CONFIG, "1",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> p = new KafkaProducer<>(config)) {
            p.send(new ProducerRecord<>(topic, 0, "conv-1", "seq=101 writer=acks1")).get();
        }
        List<String> summary = LabBroker.summarizeDump(broker.dumpLog(topic));
        r.lines(summary);
        r.lines(clientLogs.lines(l -> l.contains("acks1") && (l.contains("idempot") || l.contains("ProducerId"))));
        r.save();
        assertThat(summary.get(0)).contains("producerId=-1").contains("producerEpoch=-1");
    }
}
