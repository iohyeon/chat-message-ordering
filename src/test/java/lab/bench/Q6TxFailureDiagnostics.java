package lab.bench;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.CyclicBufferAppender;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lab.Containers;
import lab.store.StoreFixture;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Q6 (2)의 어긋남 추적. 트랜잭션당 레코드 1~10개로 쉬지 않고 커밋하면 가끔 InvalidTxnStateException으로
 * producer가 fatal 상태가 된다. 실패 직전의 클라이언트 로그(TransactionManager DEBUG, NetworkClient TRACE)를
 * 순환 버퍼에 담아 두었다가 실패하면 출력하고, 브로커는 트랜잭션 조정자 로그를 DEBUG로 켠다.
 * 어떤 요청의 응답에서 오류가 났는지, 그때 조정자는 어떤 상태였는지를 본다.
 */
@Testcontainers
@Tag("benchmark")
class Q6TxFailureDiagnostics {

    @Container
    static final KafkaContainer KAFKA = Containers.kafka()
            .withEnv("KAFKA_LOG4J_LOGGERS", "kafka.coordinator.transaction=DEBUG");

    static final int MAX_ATTEMPTS = 20;
    static final Duration PER_ATTEMPT = Duration.ofSeconds(20);

    @Test
    void reproduce() throws Exception {
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        CyclicBufferAppender<ILoggingEvent> ring = new CyclicBufferAppender<>();
        ring.setContext(ctx);
        ring.setMaxSize(400);
        ring.start();
        PatternLayoutEncoder enc = new PatternLayoutEncoder();
        enc.setContext(ctx);
        enc.setPattern("%d{HH:mm:ss.SSS} %-5level %logger{20} - %msg");
        enc.start();
        for (String name : List.of("org.apache.kafka.clients.producer.internals.TransactionManager",
                "org.apache.kafka.clients.NetworkClient")) {
            Logger l = (Logger) LoggerFactory.getLogger(name);
            l.setLevel(name.endsWith("NetworkClient") ? Level.TRACE : Level.DEBUG);
            l.setAdditive(false);
            l.addAppender(ring);
        }
        String topic = "txfail-" + System.nanoTime();
        StoreFixture.createTopic(KAFKA.getBootstrapServers(), topic);
        int failures = 0;
        long totalCommits = 0;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            String txId = "txfail-" + attempt + "-" + System.nanoTime();
            Map<String, Object> props = new HashMap<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                    ProducerConfig.LINGER_MS_CONFIG, 0,
                    ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId));
            long commits = 0;
            String where = "-";
            try (var p = new KafkaProducer<byte[], byte[]>(props)) {
                p.initTransactions();
                long end = System.nanoTime() + PER_ATTEMPT.toNanos();
                while (System.nanoTime() < end) {
                    where = "beginTransaction";
                    p.beginTransaction();
                    where = "send";
                    p.send(new ProducerRecord<>(topic, new byte[100]));
                    where = "commitTransaction";
                    p.commitTransaction();
                    commits++;
                }
            } catch (Exception e) {
                failures++;
                System.out.printf("FAIL attempt=%d txId=%s commitsBefore=%d call=%s exception=%s%n", attempt, txId,
                        commits, where, e);
                System.out.println("---- client ring buffer (last " + ring.getLength() + " events) ----");
                for (int i = 0; i < ring.getLength(); i++) {
                    String line = new String(enc.encode(ring.get(i)));
                    if (line.contains(txId) || line.contains("EndTxn") || line.contains("END_TXN")
                            || line.contains("errorCode=4") || line.contains("error")) {
                        System.out.println("  " + line.replaceAll("\\s+", " ").substring(0, Math.min(700, line.length())));
                    }
                }
                System.out.println("---- broker coordinator log for " + txId + " (last 40) ----");
                // 조정자 DEBUG 로그는 양이 많아 전체를 메모리로 읽지 않고 최근 60초만 docker CLI로 거른다.
                Process pr = new ProcessBuilder("sh", "-c",
                        "docker logs --since 60s " + KAFKA.getContainerId() + " 2>&1 | grep -F '" + txId + "'")
                        .redirectErrorStream(true).start();
                List<String> bl = new String(pr.getInputStream().readAllBytes()).lines().toList();
                pr.waitFor();
                bl.subList(Math.max(0, bl.size() - 40), bl.size())
                        .forEach(l -> System.out.println("  " + l.substring(0, Math.min(600, l.length()))));
            }
            totalCommits += commits;
            ring.reset();
            System.out.printf("ATTEMPT %d commits=%d%n", attempt, commits);
            if (failures >= 3) {
                break;
            }
        }
        System.out.printf("RESULT failures=%d totalCommits=%d%n", failures, totalCommits);
    }
}
