package lab.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 저장소 fencing 방식의 담당자. 멱등 producer만 쓰고, 레코드마다 자기 lease epoch를 헤더에 싣는다.
 * 순번은 담당자가 매긴다. {@link #accept} 는 브로커 ack를 받으면 사용자에게 성공으로 응답했다고 기록한다.
 * 저장 결과를 기다리지 않으므로, 저장에서 거부된 레코드도 호출자에게는 성공으로 보인다(Q5 (d)).
 */
public final class EpochWriter implements AutoCloseable {

    private final String name;
    private final String topic;
    private final String conversationId;
    private final long epoch;
    private final KafkaProducer<String, String> producer;
    private long nextSeq;
    private final List<ChatRecord> ackedToUser = Collections.synchronizedList(new ArrayList<>());

    public EpochWriter(String name, String bootstrap, String topic, String conversationId, long epoch, long nextSeq) {
        this.name = name;
        this.topic = topic;
        this.conversationId = conversationId;
        this.epoch = epoch;
        this.nextSeq = nextSeq;
        this.producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.CLIENT_ID_CONFIG, "writer-" + name + "-e" + epoch));
    }

    /** 다음 순번을 매겨 로그에 쓰고, 브로커 ack를 받으면 사용자에게 성공 응답을 보낸 것으로 기록한다. */
    public ChatRecord accept(String body) throws Exception {
        ChatRecord r = ChatRecord.message(conversationId, nextSeq++, epoch, body);
        RecordMetadata md = producer.send(r.toProducerRecord(topic)).get();
        if (md.offset() < 0) {
            throw new IllegalStateException("offset 없음");
        }
        ackedToUser.add(r);
        return r;
    }

    /** LOG_ORDER 변형: 담당 교체를 로그에 알리는 표시 레코드를 쓴다. 돌려주는 값은 그 레코드의 offset. */
    public long writeMarker() throws Exception {
        return producer.send(ChatRecord.marker(conversationId, epoch).toProducerRecord(topic)).get().offset();
    }

    public List<ChatRecord> ackedToUser() {
        return List.copyOf(ackedToUser);
    }

    public long epoch() {
        return epoch;
    }

    public String name() {
        return name;
    }

    @Override
    public void close() {
        producer.close();
    }
}
