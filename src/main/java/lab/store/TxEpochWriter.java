package lab.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Q8. 브로커 fencing과 저장소 세대 검사를 함께 쓰는 담당자. 트랜잭션 producer로 쓰고, 레코드마다 자기 lease epoch를
 * 헤더에 싣는다({@link ChatRecord}). 같은 대화의 담당자는 모두 같은 {@code transactional.id} 를 쓰므로,
 * 새 담당자의 {@code initTransactions()} 가 이전 담당자를 브로커에서 막는다.
 *
 * <p>사용자 응답은 {@code commitTransaction()} 결과로 정한다. 커밋이 돌아오면 그 트랜잭션의 레코드를 "성공 응답",
 * 예외가 나면 "실패 응답"으로 기록한다. 저장 결과는 기다리지 않는다.
 */
public final class TxEpochWriter implements AutoCloseable {

    private final String name;
    private final String topic;
    private final String conversationId;
    private final long epoch;
    private final KafkaProducer<String, String> producer;
    private long nextSeq;
    private final List<ChatRecord> pending = new ArrayList<>();
    private final List<ChatRecord> ackedToUser = Collections.synchronizedList(new ArrayList<>());
    private final List<ChatRecord> failedToUser = Collections.synchronizedList(new ArrayList<>());

    public TxEpochWriter(String name, String bootstrap, String topic, String conversationId, String transactionalId,
                         long epoch, Map<String, Object> overrides) {
        this.name = name;
        this.topic = topic;
        this.conversationId = conversationId;
        this.epoch = epoch;
        Map<String, Object> cfg = new HashMap<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId,
                ProducerConfig.CLIENT_ID_CONFIG, "txwriter-" + name + "-e" + epoch));
        cfg.putAll(overrides);
        this.producer = new KafkaProducer<>(cfg);
    }

    public void initTransactions() {
        producer.initTransactions();
    }

    /** 순번을 정한다. 새 담당자는 replay로 본 마지막 순번 다음부터 매긴다. */
    public void startAt(long seq) {
        this.nextSeq = seq;
    }

    /**
     * 트랜잭션을 열고 본문마다 다음 순번을 매겨 보낸 뒤 {@code flush()} 한다. 브로커 ack까지 받지만 커밋은 하지 않는다.
     * 돌려주는 값은 보낸 레코드들.
     */
    public List<ChatRecord> beginAndSend(List<String> bodies) throws Exception {
        if (!pending.isEmpty()) {
            throw new IllegalStateException("커밋되지 않은 트랜잭션이 있다");
        }
        producer.beginTransaction();
        List<ChatRecord> sent = new ArrayList<>();
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (String body : bodies) {
            ChatRecord r = ChatRecord.message(conversationId, nextSeq++, epoch, body);
            futures.add(producer.send(r.toProducerRecord(topic)));
            sent.add(r);
        }
        producer.flush();
        for (var f : futures) {
            f.get();
        }
        pending.addAll(sent);
        return sent;
    }

    /**
     * 열린 트랜잭션을 커밋한다. 성공하면 그 레코드들을 성공 응답으로, 예외가 나면 실패 응답으로 기록하고 예외를 다시 던진다.
     */
    public void commit() {
        List<ChatRecord> batch = List.copyOf(pending);
        pending.clear();
        try {
            producer.commitTransaction();
        } catch (RuntimeException e) {
            failedToUser.addAll(batch);
            throw e;
        }
        ackedToUser.addAll(batch);
    }

    /** 본문들을 한 트랜잭션으로 보내고 커밋한다. */
    public List<ChatRecord> accept(List<String> bodies) throws Exception {
        List<ChatRecord> sent = beginAndSend(bodies);
        commit();
        return sent;
    }

    /** 교체 표시 레코드를 트랜잭션 하나로 쓰고 커밋한다. */
    public void writeMarker() throws Exception {
        producer.beginTransaction();
        producer.send(ChatRecord.marker(conversationId, epoch).toProducerRecord(topic)).get();
        producer.commitTransaction();
    }

    public List<ChatRecord> ackedToUser() {
        return List.copyOf(ackedToUser);
    }

    public List<ChatRecord> failedToUser() {
        return List.copyOf(failedToUser);
    }

    public long epoch() {
        return epoch;
    }

    /** 다음에 매길 순번. */
    public long nextSeq() {
        return nextSeq;
    }

    public String name() {
        return name;
    }

    public KafkaProducer<String, String> producer() {
        return producer;
    }

    @Override
    public void close() {
        producer.close(java.time.Duration.ofSeconds(5));
    }
}
