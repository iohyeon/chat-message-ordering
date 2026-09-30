package lab;

import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** 시나리오 테스트가 함께 쓰는 컨테이너 이미지 버전. */
public final class Containers {

    public static final String KAFKA_IMAGE = "apache/kafka:4.3.1";
    public static final String POSTGRES_IMAGE = "postgres:16";

    private Containers() {
    }

    public static KafkaContainer kafka() {
        return new KafkaContainer(KAFKA_IMAGE);
    }

    public static PostgreSQLContainer postgres() {
        return new PostgreSQLContainer(POSTGRES_IMAGE);
    }
}
