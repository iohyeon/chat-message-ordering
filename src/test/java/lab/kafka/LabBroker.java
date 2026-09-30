package lab.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lab.Containers;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.kafka.KafkaContainer;

/**
 * 실험에서 쓰는 브로커 한 대와 관찰 도구.
 *
 * <p>브로커 쪽 근거를 남기려고 요청 로그({@code kafka.request.logger})와 트랜잭션 조정자 로그를 DEBUG로 켠다.
 * 요청 로그에는 요청마다 API 이름, 요청 본문, 응답 본문(에러 코드 포함)이 JSON으로 찍힌다.
 */
final class LabBroker implements AutoCloseable {

    static final String LOG_DIR = "/tmp/kafka-logs";

    final KafkaContainer container;
    final Admin admin;

    private LabBroker(KafkaContainer container) {
        this.container = container;
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, container.getBootstrapServers());
        this.admin = Admin.create(p);
    }

    static LabBroker start() {
        return start(Map.of());
    }

    static LabBroker start(Map<String, String> extraEnv) {
        KafkaContainer k = Containers.kafka()
                .withEnv("KAFKA_LOG4J_LOGGERS",
                        "kafka.request.logger=DEBUG,kafka.coordinator.transaction=DEBUG,"
                                + "org.apache.kafka.coordinator.transaction=DEBUG,kafka.server.ReplicaManager=DEBUG,"
                                + "kafka.server.AddPartitionsToTxnManager=DEBUG");
        extraEnv.forEach(k::withEnv);
        k.start();
        return new LabBroker(k);
    }

    String bootstrap() {
        return container.getBootstrapServers();
    }

    void createTopic(String topic) throws Exception {
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
    }

    /**
     * LSO가 HW를 따라잡을 때까지 기다린다. TV2에서 commitTransaction() 은 조정자가 PREPARE_COMMIT 을 기록한 뒤
     * 반환하고 마커는 그 뒤에 쓰이므로, 커밋 직후 read_committed 로 읽으면 마지막 트랜잭션이 아직 안 보일 수 있다.
     */
    void awaitStable(String topic) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        while (endOffset(topic, IsolationLevel.READ_COMMITTED) < endOffset(topic, IsolationLevel.READ_UNCOMMITTED)
                && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
    }

    /** 파티션 0을 처음부터 끝까지 읽는다. 끝은 그 격리 수준에서 본 endOffset(read_committed면 LSO)이다. */
    List<ConsumerRecord<String, String>> readAll(String topic, IsolationLevel isolation) {
        return readAll(topic, isolation, Duration.ofSeconds(20)).records();
    }

    ReadResult readAll(String topic, IsolationLevel isolation, Duration timeout) {
        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        c.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation.toString());
        c.put(ConsumerConfig.CLIENT_ID_CONFIG, "reader-" + isolation.toString().replace('_', '-') + "@" + topic);
        c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        TopicPartition tp = new TopicPartition(topic, 0);
        List<ConsumerRecord<String, String>> out = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(c)) {
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            long end = consumer.endOffsets(List.of(tp)).get(tp);
            long deadline = System.nanoTime() + timeout.toNanos();
            while (consumer.position(tp) < end && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(out::add);
            }
            return new ReadResult(out, end, consumer.position(tp));
        }
    }

    record ReadResult(List<ConsumerRecord<String, String>> records, long endOffset, long finalPosition) {
    }

    /** 격리 수준별 끝 offset. read_uncommitted는 high watermark, read_committed는 LSO를 돌려준다. */
    long endOffset(String topic, IsolationLevel isolation) throws Exception {
        TopicPartition tp = new TopicPartition(topic, 0);
        return admin.listOffsets(Map.of(tp, OffsetSpec.latest()), new ListOffsetsOptions(isolation))
                .partitionResult(tp).get(30, TimeUnit.SECONDS).offset();
    }

    TransactionDescription describeTxn(String transactionalId) throws Exception {
        return admin.describeTransactions(List.of(transactionalId)).description(transactionalId).get(30, TimeUnit.SECONDS);
    }

    String describeTxnLine(String transactionalId) {
        try {
            TransactionDescription d = describeTxn(transactionalId);
            return "state=" + d.state() + " producerId=" + d.producerId() + " producerEpoch=" + d.producerEpoch()
                    + " topicPartitions=" + d.topicPartitions();
        } catch (Exception e) {
            return "describeTransactions 실패: " + e;
        }
    }

    /** 파티션 리더가 기억하는 producer 상태(producer ID별 마지막 epoch, seq, 열린 트랜잭션 시작 offset). */
    List<ProducerState> describeProducers(String topic) throws Exception {
        TopicPartition tp = new TopicPartition(topic, 0);
        return admin.describeProducers(List.of(tp)).partitionResult(tp).get(30, TimeUnit.SECONDS).activeProducers();
    }

    String describeProducersLine(String topic) {
        try {
            StringBuilder sb = new StringBuilder();
            for (ProducerState s : describeProducers(topic)) {
                sb.append("[producerId=").append(s.producerId())
                        .append(" producerEpoch=").append(s.producerEpoch())
                        .append(" lastSequence=").append(s.lastSequence())
                        .append(" currentTransactionStartOffset=").append(s.currentTransactionStartOffset())
                        .append("] ");
            }
            return sb.toString().trim();
        } catch (Exception e) {
            return "describeProducers 실패: " + e;
        }
    }

    /** 컨테이너 안에서 kafka-dump-log로 세그먼트를 읽는다. 배치 헤더의 producerId, producerEpoch, 마커가 보인다. */
    String dumpLog(String topic) throws Exception {
        ExecResult r = container.execInContainer("bash", "-c",
                "/opt/kafka/bin/kafka-dump-log.sh --print-data-log --files " + LOG_DIR + "/" + topic
                        + "-0/00000000000000000000.log 2>&1");
        return r.getStdout() + r.getStderr();
    }

    /** dump-log 결과에서 배치 줄과 레코드 줄만 추려 한 줄씩 요약한다. */
    static List<String> summarizeDump(String dump) {
        List<String> out = new ArrayList<>();
        for (String line : dump.split("\n")) {
            if (line.startsWith("baseOffset:")) {
                out.add(pick(line, "baseOffset", "lastOffset", "baseSequence", "lastSequence", "producerId",
                        "producerEpoch", "isTransactional", "isControl"));
            } else if (line.startsWith("| offset:")) {
                String payload = line.contains("payload:") ? line.substring(line.indexOf("payload:")) : "";
                String marker = line.contains("endTxnMarker:") ? line.substring(line.indexOf("endTxnMarker:")) : "";
                out.add("    | " + pick(line, "offset", "sequence") + " " + (marker.isEmpty() ? payload : marker));
            }
        }
        return out;
    }

    private static String pick(String line, String... keys) {
        StringBuilder sb = new StringBuilder();
        String[] tokens = line.replace("| ", "").trim().split(" ");
        for (int i = 0; i + 1 < tokens.length; i++) {
            for (String k : keys) {
                if (tokens[i].equals(k + ":")) {
                    sb.append(k).append('=').append(tokens[i + 1]).append(' ');
                }
            }
        }
        return sb.toString().trim();
    }

    /** 브로커 표준 출력 전체(docker logs 와 같은 내용). */
    String brokerLogs() {
        return container.getLogs();
    }

    /**
     * 브로커 요청 로그(kafka.request.logger DEBUG)에서 관심 있는 API만 추려 한 줄로 줄인다.
     * keys 중 하나라도 들어 있는 줄만 남긴다(transactional.id, 토픽 이름 등).
     */
    List<String> requests(List<String> apis, String... keys) {
        return requestLines(apis, keys).stream().map(RequestLine::render).toList();
    }

    List<RequestLine> requestLines(List<String> apis, String... keys) {
        List<RequestLine> out = new ArrayList<>();
        for (String l : brokerLogs().split("\n")) {
            if (!l.contains("Completed request:")) {
                continue;
            }
            String api = between(l, "\"requestApiKeyName\":\"", "\"");
            if (!apis.contains(api)) {
                continue;
            }
            boolean match = keys.length == 0;
            for (String k : keys) {
                match |= l.contains(k);
            }
            if (!match) {
                continue;
            }
            out.add(new RequestLine(l.substring(1, l.indexOf(']')), api, between(l, "\"requestApiVersion\":", ","),
                    between(l, "\"clientId\":\"", "\""), between(l, "\"request\":", ",\"response\":"),
                    between(l, "\"response\":", ",\"connection\":")));
        }
        return out;
    }

    /** 요청 로그 한 줄. 에러 코드는 errorCode, partitionErrorCode 를 응답에 나온 순서대로 모은 것이다. */
    record RequestLine(String time, String api, String version, String clientId, String request, String response) {
        List<Integer> errors() {
            List<Integer> out = new ArrayList<>();
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"(?:partitionE|e)rrorCode\":(-?\\d+)").matcher(response);
            while (m.find()) {
                out.add(Integer.parseInt(m.group(1)));
            }
            return out;
        }

        String render() {
            return time + " " + api + " v" + version + " clientId=" + clientId + "\n      request=" + request
                    + "\n      response=" + response;
        }
    }

    /** 브로커 로그에서 조건에 맞는 줄(요청 로그 제외). */
    List<String> brokerLines(java.util.function.Predicate<String> filter) {
        List<String> out = new ArrayList<>();
        for (String l : brokerLogs().split("\n")) {
            if (!l.contains("Completed request:") && filter.test(l)) {
                out.add(l);
            }
        }
        return out;
    }

    private static String between(String s, String start, String end) {
        int i = s.indexOf(start);
        if (i < 0) {
            return "";
        }
        i += start.length();
        int j = s.indexOf(end, i);
        return j < 0 ? s.substring(i) : s.substring(i, j);
    }

    @Override
    public void close() {
        admin.close(Duration.ofSeconds(5));
        container.stop();
    }
}
