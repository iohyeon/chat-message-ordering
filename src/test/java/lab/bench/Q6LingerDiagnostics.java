package lab.bench;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import lab.Containers;
import lab.store.StoreFixture;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Q6 (2)의 어긋남 추적. 레코드 하나씩 send().get()으로 보낼 때 linger.ms=5면 레코드당 약 7.9ms였다(linger 0은 0.43ms).
 * linger.ms 값을 바꿔 가며 send().get() 시간을 재서, 늘어나는 양이 linger.ms 그대로인지 그보다 큰지 본다.
 */
@Testcontainers
@Tag("benchmark")
class Q6LingerDiagnostics {

    @Container
    static final KafkaContainer KAFKA = Containers.kafka();

    @Test
    void syncSendVsLinger() throws Exception {
        String topic = "linger-" + System.nanoTime();
        StoreFixture.createTopic(KAFKA.getBootstrapServers(), topic);
        for (int linger : new int[] {0, 1, 2, 5, 10, 20, 0, 5}) {
            try (var p = new KafkaProducer<byte[], byte[]>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                    ProducerConfig.LINGER_MS_CONFIG, linger))) {
                for (int i = 0; i < 50; i++) {
                    p.send(new ProducerRecord<>(topic, new byte[100])).get();
                }
                int n = 300;
                long[] t = new long[n];
                for (int i = 0; i < n; i++) {
                    long a = System.nanoTime();
                    p.send(new ProducerRecord<>(topic, new byte[100])).get();
                    t[i] = System.nanoTime() - a;
                }
                Arrays.sort(t);
                String line = String.format(Locale.ROOT, "%d,%d,%.3f,%.3f,%.3f,%.3f", linger, n, t[n / 10] / 1e6,
                        t[n / 2] / 1e6, t[n * 9 / 10] / 1e6, t[n - 1] / 1e6);
                System.out.println("RESULT " + line);
                Bench.append("q6_2_linger_diag.csv", "linger_ms,n,p10_ms,p50_ms,p90_ms,max_ms", line);
            }
        }
    }
}
