package lab.store;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Q8. 새 담당자가 {@code initTransactions()} 뒤에 로그를 read_committed 로 끝까지 읽어 마지막으로 커밋된 순번을 찾는다.
 * {@link ChatRecord} 형식을 읽고 교체 표시는 건너뛴다. 끝은 시작 시점의 {@code endOffsets()}(read_committed에서는 LSO)다.
 */
public final class CommittedReplay {

    private CommittedReplay() {
    }

    public record Result(long lastSeq, long endOffset, long finalPosition, int records) {
    }

    public static Result replay(String bootstrap, String topic, String clientId, Duration timeout) {
        TopicPartition tp = new TopicPartition(topic, 0);
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.CLIENT_ID_CONFIG, clientId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"))) {
            consumer.assign(Set.of(tp));
            consumer.seekToBeginning(Set.of(tp));
            long end = consumer.endOffsets(Set.of(tp)).get(tp);
            long deadline = System.nanoTime() + timeout.toNanos();
            long last = -1;
            int n = 0;
            while (consumer.position(tp) < end) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("replay가 끝(" + end + ")까지 가지 못했다. position=" + consumer.position(tp));
                }
                for (var cr : consumer.poll(Duration.ofMillis(200))) {
                    ChatRecord r = ChatRecord.from(cr);
                    if (!r.marker()) {
                        last = Math.max(last, r.seq());
                        n++;
                    }
                }
            }
            return new Result(last, end, consumer.position(tp), n);
        }
    }

    /** read_committed로 로그 전체를 읽는다(검증용). */
    public static List<ChatRecord> readAll(String bootstrap, String topic, Duration timeout) {
        TopicPartition tp = new TopicPartition(topic, 0);
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"))) {
            consumer.assign(Set.of(tp));
            consumer.seekToBeginning(Set.of(tp));
            long end = consumer.endOffsets(Set.of(tp)).get(tp);
            long deadline = System.nanoTime() + timeout.toNanos();
            List<ChatRecord> out = new ArrayList<>();
            while (consumer.position(tp) < end && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(cr -> out.add(ChatRecord.from(cr)));
            }
            if (consumer.position(tp) < end) {
                throw new IllegalStateException("끝(" + end + ")까지 읽지 못했다");
            }
            return out;
        }
    }
}
