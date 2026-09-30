package lab.shard;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 새 담당자가 수락을 시작하기 전에 로그를 read_committed 로 끝까지 읽어 마지막으로 커밋된 seq를 찾는다.
 *
 * <p>"끝"은 시작 시점에 {@code endOffsets()} 가 돌려준 값이다. read_committed 소비자에게 이 값은 high watermark가 아니라
 * LSO(last stable offset)다. 열린 트랜잭션이 있으면 LSO는 그 트랜잭션의 첫 offset에 머문다.
 */
public final class SeqReplay {

    private SeqReplay() {
    }

    public record Result(long lastSeq, long endOffset, long finalPosition, List<String> seen) {
    }

    public static Result replay(String bootstrap, String topic, String clientId, Duration timeout) {
        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        c.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        c.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString());
        c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        TopicPartition tp = new TopicPartition(topic, 0);
        long lastSeq = -1;
        List<String> seen = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(c)) {
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            long end = consumer.endOffsets(List.of(tp)).get(tp);
            long deadline = System.nanoTime() + timeout.toNanos();
            while (consumer.position(tp) < end && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> rec : consumer.poll(Duration.ofMillis(200))) {
                    seen.add(rec.offset() + ":" + rec.value());
                    lastSeq = Math.max(lastSeq, parseSeq(rec.value()));
                }
            }
            return new Result(lastSeq, end, consumer.position(tp), seen);
        }
    }

    static long parseSeq(String value) {
        return Long.parseLong(value.substring("seq=".length(), value.indexOf(' ')));
    }
}
