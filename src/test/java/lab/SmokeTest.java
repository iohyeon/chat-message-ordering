package lab;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.DriverManager;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** 컨테이너 런타임과 두 이미지가 이 환경에서 뜨는지만 확인한다. */
@Testcontainers
class SmokeTest {

    @Container
    static final KafkaContainer KAFKA = Containers.kafka();

    @Container
    static final PostgreSQLContainer POSTGRES = Containers.postgres();

    @Test
    void kafkaRoundTrip() throws Exception {
        Map<String, Object> p = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(p)) {
            producer.send(new ProducerRecord<>("smoke", "k", "v")).get();
        }
        Map<String, Object> c = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "smoke",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (var consumer = new KafkaConsumer<String, String>(c)) {
            consumer.subscribe(List.of("smoke"));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isEqualTo(1);
        }
    }

    @Test
    void postgresSelect() throws Exception {
        try (var conn = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var rs = conn.createStatement().executeQuery("select version()")) {
            assertThat(rs.next()).isTrue();
            System.out.println("postgres: " + rs.getString(1));
        }
    }
}
