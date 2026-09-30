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

/** 토픽 파티션 0을 처음부터 끝까지 읽어 로그에 무엇이 남았는지 확인한다. */
final class LogReader {

    private LogReader() {
    }

    static List<ChatRecord> readAll(String bootstrap, String topic) {
        TopicPartition tp = new TopicPartition(topic, 0);
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false))) {
            consumer.assign(Set.of(tp));
            consumer.seekToBeginning(Set.of(tp));
            long end = consumer.endOffsets(Set.of(tp)).get(tp);
            List<ChatRecord> out = new ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (consumer.position(tp) < end && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(r -> out.add(ChatRecord.from(r)));
            }
            return out;
        }
    }
}
