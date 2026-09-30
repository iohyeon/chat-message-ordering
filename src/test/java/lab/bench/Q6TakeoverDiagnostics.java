package lab.bench;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lab.Containers;
import lab.store.ChatRecord;
import lab.store.StoreFixture;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Q6 (4) 결과의 두 어긋남을 추적한다.
 * <ol>
 *   <li>옛 담당자가 깨끗이 닫았는데도 initTransactions()가 약 120ms 걸리는 이유. 클라이언트 TransactionManager의 DEBUG 로그로
 *       브로커 응답 오류와 재시도를 본다.</li>
 *   <li>read_committed replay가 약 530ms 걸리는 이유. poll 한 번씩의 시간과 위치를 fetch.max.wait.ms 두 값으로 비교한다.</li>
 * </ol>
 */
@Testcontainers
@Tag("benchmark")
class Q6TakeoverDiagnostics {

    @Container
    static final KafkaContainer KAFKA = Containers.kafka();

    static Map<String, Object> producerProps(String txId) {
        Map<String, Object> p = new HashMap<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.LINGER_MS_CONFIG, 0));
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
        return p;
    }

    @Test
    void initTransactionsAndReplay() throws Exception {
        String topic = "diag-" + System.nanoTime();
        StoreFixture.createTopic(KAFKA.getBootstrapServers(), topic);
        String txId = "diag-shard";
        try (var p = new KafkaProducer<String, String>(producerProps(txId))) {
            p.initTransactions();
            p.beginTransaction();
            for (int i = 1; i <= 1000; i++) {
                p.send(ChatRecord.message("c", i, 1, "m").toProducerRecord(topic));
            }
            p.commitTransaction();
        }
        Logger tm = (Logger) LoggerFactory.getLogger("org.apache.kafka.clients.producer.internals.TransactionManager");
        Logger sender = (Logger) LoggerFactory.getLogger("org.apache.kafka.clients.producer.internals.Sender");
        for (String when : List.of("closed-just-now", "closed-2s-ago")) {
            if (when.equals("closed-2s-ago")) {
                Thread.sleep(2000);
            }
            tm.setLevel(Level.DEBUG);
            sender.setLevel(Level.DEBUG);
            System.out.println("---- initTransactions, previous producer " + when);
            var p = new KafkaProducer<String, String>(producerProps(txId));
            long t0 = System.nanoTime();
            p.initTransactions();
            System.out.printf("DIAG initTransactions(%s) = %.1fms%n", when, (System.nanoTime() - t0) / 1e6);
            long t1 = System.nanoTime();
            p.beginTransaction();
            p.send(ChatRecord.message("c", 2000, 2, "m").toProducerRecord(topic));
            p.commitTransaction();
            System.out.printf("DIAG first commit(%s) = %.1fms%n", when, (System.nanoTime() - t1) / 1e6);
            tm.setLevel(Level.WARN);
            sender.setLevel(Level.WARN);
            p.close();
        }

        // 옛 담당자가 트랜잭션을 열어 둔 채 멈춘 경우.
        var zombie = new KafkaProducer<String, String>(producerProps(txId));
        zombie.initTransactions();
        zombie.beginTransaction();
        zombie.send(ChatRecord.message("c", 3000, 3, "zombie").toProducerRecord(topic)).get();
        tm.setLevel(Level.DEBUG);
        sender.setLevel(Level.DEBUG);
        Logger net = (Logger) LoggerFactory.getLogger("org.apache.kafka.clients.NetworkClient");
        net.setLevel(Level.TRACE);
        System.out.println("---- initTransactions, previous producer has an open transaction");
        var fresh = new KafkaProducer<String, String>(producerProps(txId));
        long t0 = System.nanoTime();
        fresh.initTransactions();
        System.out.printf("DIAG initTransactions(open) = %.1fms%n", (System.nanoTime() - t0) / 1e6);
        net.setLevel(Level.WARN);
        tm.setLevel(Level.WARN);
        sender.setLevel(Level.WARN);
        replay(topic, 500);
        fresh.close();
        zombie.close(Duration.ZERO);

        for (int maxWait : new int[] {500, 50}) {
            for (int round = 0; round < 3; round++) {
                replay(topic, maxWait);
            }
        }
    }

    static void replay(String topic, int fetchMaxWaitMs) {
        TopicPartition tp = new TopicPartition(topic, 0);
        long t0 = System.nanoTime();
        List<String> polls = new ArrayList<>();
        var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitMs));
        consumer.assign(Set.of(tp));
        long tCreate = System.nanoTime();
        long lso = consumer.endOffsets(Set.of(tp)).get(tp);
        long tEnd = System.nanoTime();
        consumer.seek(tp, Math.max(0, lso - 50));
        while (consumer.position(tp) < lso) {
            long a = System.nanoTime();
            int n = consumer.poll(Duration.ofMillis(100)).count();
            polls.add(String.format("poll %.1fms n=%d pos=%d", (System.nanoTime() - a) / 1e6, n, consumer.position(tp)));
        }
        long tRead = System.nanoTime();
        consumer.close();
        long tClose = System.nanoTime();
        System.out.printf("DIAG replay fetch.max.wait.ms=%d: create=%.1fms endOffsets=%.1fms(lso=%d) read=%.1fms close=%.1fms %s%n",
                fetchMaxWaitMs, (tCreate - t0) / 1e6, (tEnd - tCreate) / 1e6, lso, (tRead - t0) / 1e6,
                (tClose - tRead) / 1e6, polls);
    }
}
