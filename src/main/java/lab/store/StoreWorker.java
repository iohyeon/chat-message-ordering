package lab.store;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 저장 워커. 파티션 하나를 처음부터 순서대로 읽어 레코드마다 {@link FenceMode} 방식으로 message 테이블에 쓴다.
 * 레코드 하나가 autocommit 문장 하나다. 컨슈머 그룹을 쓰지 않고 파티션을 직접 배정한다(재배정 지연을 빼기 위해).
 */
public final class StoreWorker implements AutoCloseable {

    public record Processed(long offset, ChatRecord record, MessageStore.Outcome outcome) {
    }

    private final KafkaConsumer<String, String> consumer;
    private final TopicPartition tp;
    private final DataSource dataSource;
    private final FenceMode mode;

    public StoreWorker(String bootstrap, String topic, DataSource dataSource, FenceMode mode) {
        this.consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                // 멱등 producer만 쓰므로 트랜잭션이 없다. 기본값 그대로 둔다는 것을 드러내려고 적는다.
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted"));
        this.tp = new TopicPartition(topic, 0);
        this.dataSource = dataSource;
        this.mode = mode;
        consumer.assign(Set.of(tp));
        consumer.seekToBeginning(Set.of(tp));
    }

    /** 지금 로그 끝까지 처리하고 처리 결과를 순서대로 돌려준다. */
    public List<Processed> drainToEnd(Duration timeout) throws SQLException {
        long end = consumer.endOffsets(Set.of(tp)).get(tp);
        long deadline = System.nanoTime() + timeout.toNanos();
        List<Processed> out = new ArrayList<>();
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(true);
            while (consumer.position(tp) < end) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("로그 끝(" + end + ")까지 읽지 못했다. position=" + consumer.position(tp));
                }
                for (var cr : consumer.poll(Duration.ofMillis(200))) {
                    ChatRecord r = ChatRecord.from(cr);
                    MessageStore.Outcome outcome;
                    if (r.marker()) {
                        if (mode == FenceMode.LOG_ORDER) {
                            MessageStore.bumpFence(c, r.conversationId(), r.epoch());
                        }
                        outcome = null;
                    } else {
                        int n = MessageStore.insert(c, mode, r);
                        outcome = MessageStore.classify(c, mode, r, n);
                    }
                    out.add(new Processed(cr.offset(), r, outcome));
                }
            }
        }
        return out;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
