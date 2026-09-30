package lab.store;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

/**
 * 로그에 싣는 채팅 레코드. 키는 대화 id, 값은 본문이고, 순번과 lease epoch는 헤더에 싣는다.
 * {@code marker} 는 LOG_ORDER 변형에서 새 담당자가 교체를 로그에 알리는 레코드다(본문 없음).
 */
public record ChatRecord(String conversationId, long seq, long epoch, String body, boolean marker) {

    public static final String H_SEQ = "seq";
    public static final String H_EPOCH = "epoch";
    public static final String H_MARKER = "marker";

    public static ChatRecord message(String conversationId, long seq, long epoch, String body) {
        return new ChatRecord(conversationId, seq, epoch, body, false);
    }

    public static ChatRecord marker(String conversationId, long epoch) {
        return new ChatRecord(conversationId, -1, epoch, "", true);
    }

    public ProducerRecord<String, String> toProducerRecord(String topic) {
        var r = new ProducerRecord<String, String>(topic, conversationId, body);
        r.headers().add(H_SEQ, Long.toString(seq).getBytes(StandardCharsets.UTF_8));
        r.headers().add(H_EPOCH, Long.toString(epoch).getBytes(StandardCharsets.UTF_8));
        if (marker) {
            r.headers().add(H_MARKER, new byte[] {1});
        }
        return r;
    }

    public static ChatRecord from(ConsumerRecord<String, String> r) {
        return new ChatRecord(r.key(), header(r, H_SEQ), header(r, H_EPOCH), r.value(),
                r.headers().lastHeader(H_MARKER) != null);
    }

    private static long header(ConsumerRecord<String, String> r, String key) {
        Header h = r.headers().lastHeader(key);
        if (h == null) {
            throw new IllegalArgumentException("헤더 없음: " + key + " at offset " + r.offset());
        }
        return Long.parseLong(new String(h.value(), StandardCharsets.UTF_8));
    }
}
