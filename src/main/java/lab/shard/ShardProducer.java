package lab.shard;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 대화 하나의 순번(conversation_seq)을 매겨 Kafka에 기록하는 담당자 흉내.
 *
 * <p>레코드 key는 대화 id, value는 {@code seq=<n> writer=<이름>}, 헤더에 {@code seq} 와 {@code writer} 를 싣는다.
 * producer ID와 epoch는 레코드 헤더가 아니라 배치 헤더에 실리므로 소비자 API로는 보이지 않는다.
 * 그래서 클라이언트 쪽 값은 {@link #producerIdAndEpoch()} 로, 로그 쪽 값은 kafka-dump-log로 확인한다.
 */
public final class ShardProducer implements AutoCloseable {

    public static final String CONVERSATION_ID = "conv-1";

    private final String writer;
    private final String topic;
    private final KafkaProducer<String, String> producer;

    private ShardProducer(String writer, String topic, Map<String, Object> config) {
        this.writer = writer;
        this.topic = topic;
        this.producer = new KafkaProducer<>(config);
    }

    /**
     * 멱등 producer. {@code enable.idempotence} 는 일부러 설정하지 않는다. Kafka 3.0부터 기본값이 true이다.
     * 이 값을 명시하지 않았을 때 acks=0/1 이나 retries=0 을 주면 멱등이 조용히 꺼진다. max.in.flight.requests.per.connection 이
     * 5를 넘으면 꺼지지 않고 생성 시점에 {@code ConfigException} 이 난다(ProducerConfig 4.3.1 L620-L623).
     * 기본값으로 켜지는지를 실험에서 확인한다.
     */
    public static ShardProducer idempotent(String bootstrap, String writer, String topic) {
        return new ShardProducer(writer, topic, baseConfig(bootstrap, writer));
    }

    /** 트랜잭션 producer. {@code transactional.id} 를 주면 멱등이 강제로 켜진다. */
    public static ShardProducer transactional(String bootstrap, String writer, String topic, String transactionalId,
                                              Map<String, Object> overrides) {
        Map<String, Object> config = baseConfig(bootstrap, writer);
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        config.putAll(overrides);
        return new ShardProducer(writer, topic, config);
    }

    private static Map<String, Object> baseConfig(String bootstrap, String writer) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "writer-" + writer);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return config;
    }

    public String writer() {
        return writer;
    }

    public KafkaProducer<String, String> producer() {
        return producer;
    }

    /** seq 하나를 보낸다. 반환값에 {@code send()} 가 돌려준 Future와 콜백 결과를 함께 담는다. */
    public Sent send(long seq) {
        CompletableFuture<CallbackResult> callback = new CompletableFuture<>();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, 0, CONVERSATION_ID,
                "seq=" + seq + " writer=" + writer,
                List.of(new RecordHeader("seq", Long.toString(seq).getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader("writer", writer.getBytes(StandardCharsets.UTF_8))));
        Future<RecordMetadata> future = producer.send(record, (metadata, exception) ->
                callback.complete(new CallbackResult(metadata, exception, Thread.currentThread().getName())));
        return new Sent(seq, future, callback);
    }

    /** 클라이언트가 지금 들고 있는 producer ID와 epoch. 내부 필드라 리플렉션으로 읽는다. */
    public String producerIdAndEpoch() {
        return producerIdAndEpoch(producer);
    }

    /** 클라이언트 TransactionManager의 현재 상태(READY, IN_TRANSACTION, ABORTABLE_ERROR, FATAL_ERROR 등). */
    public String transactionState() {
        return transactionState(producer);
    }

    /** {@link #producerIdAndEpoch()} 를 아무 producer에나 쓴다(Q9). */
    public static String producerIdAndEpoch(KafkaProducer<?, ?> producer) {
        try {
            Field field = KafkaProducer.class.getDeclaredField("transactionManager");
            field.setAccessible(true);
            Object transactionManager = field.get(producer);
            if (transactionManager == null) {
                return "(transactionManager 없음: 멱등이 꺼져 있다)";
            }
            Method method = transactionManager.getClass().getDeclaredMethod("producerIdAndEpoch");
            method.setAccessible(true);
            return method.invoke(transactionManager).toString();
        } catch (ReflectiveOperationException e) {
            return "(읽지 못함: " + e + ")";
        }
    }

    /** {@link #transactionState()} 를 아무 producer에나 쓴다(Q9). */
    public static String transactionState(KafkaProducer<?, ?> producer) {
        try {
            Field field = KafkaProducer.class.getDeclaredField("transactionManager");
            field.setAccessible(true);
            Object transactionManager = field.get(producer);
            if (transactionManager == null) {
                return "(없음)";
            }
            Field state = transactionManager.getClass().getDeclaredField("currentState");
            state.setAccessible(true);
            Field lastError = transactionManager.getClass().getDeclaredField("lastError");
            lastError.setAccessible(true);
            Object error = lastError.get(transactionManager);
            return state.get(transactionManager) + (error != null ? " lastError=" + error.getClass().getSimpleName() : "");
        } catch (ReflectiveOperationException e) {
            return "(읽지 못함: " + e + ")";
        }
    }

    @Override
    public void close() {
        producer.close(java.time.Duration.ofSeconds(5));
    }

    public record Sent(long seq, Future<RecordMetadata> future, CompletableFuture<CallbackResult> callback) {
    }

    public record CallbackResult(RecordMetadata metadata, Exception exception, String thread) {
    }
}
