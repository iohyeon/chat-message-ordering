package lab.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lab.Containers;
import lab.lease.LeaseRepository;
import lab.store.MessageStore.Outcome;
import lab.store.MessageStore.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Q5. 멱등 producer만 쓰고 레코드에 lease epoch를 실어, 저장 워커의 조건부 INSERT가 옛 담당자의 레코드를 거르는지 본다.
 * 담당자 A(epoch 5)와 B(epoch 6)는 같은 JVM의 서로 다른 producer다. A의 "멈춤"은 호출을 하지 않는 것으로 흉내 낸다.
 * lease 만료는 {@link LeaseRepository#forceExpire} 로 만든다. A는 만료를 통보받지 않는다.
 */
@Testcontainers
class Q5StoreFencingTest {

    @Container
    static final KafkaContainer KAFKA = Containers.kafka();

    @Container
    static final PostgreSQLContainer POSTGRES = Containers.postgres();

    static HikariDataSource ds;
    static LeaseRepository leases;
    static MessageStore store;

    String topic;
    String conv;
    Report r;

    @BeforeAll
    static void setUp() throws Exception {
        ds = StoreFixture.dataSource(POSTGRES, 4);
        StoreFixture.applySchema(ds);
        leases = new LeaseRepository(ds);
        store = new MessageStore(ds);
    }

    @AfterAll
    static void tearDown() {
        ds.close();
    }

    @BeforeEach
    void each(TestInfo info) throws Exception {
        r = new Report("Q5-" + info.getTestMethod().orElseThrow().getName());
        String id = UUID.randomUUID().toString().substring(0, 8);
        topic = "chat-log-" + id;
        conv = "conv-" + id;
        StoreFixture.createTopic(KAFKA.getBootstrapServers(), topic);
        // 교체 전에 seq 100까지 저장되어 있고, lease는 epoch 4에서 만료된 상태로 시작한다.
        leases.create(conv, 4);
        StoreFixture.seed(ds, conv, 100, 4);
        r.line("=== " + info.getDisplayName() + " topic=" + topic + " conv=" + conv);
    }

    @AfterEach
    void save() throws Exception {
        r.save();
    }

    EpochWriter acquire(String name, long nextSeq) throws Exception {
        long epoch = leases.tryAcquire(conv, name, Duration.ofSeconds(30)).orElseThrow();
        r.line(name + " lease 획득 epoch=" + epoch + " nextSeq=" + nextSeq);
        return new EpochWriter(name, KAFKA.getBootstrapServers(), topic, conv, epoch, nextSeq);
    }

    void print(String title, List<StoreWorker.Processed> ps) {
        r.line("-- " + title);
        for (var p : ps) {
            var rec = p.record();
            r.printf("   offset=%d %s seq=%d epoch=%d body=%s -> %s%n", p.offset(),
                    rec.marker() ? "MARKER" : "record", rec.seq(), rec.epoch(), rec.body(), p.outcome());
        }
    }

    void printRows() throws Exception {
        r.line("-- message 테이블");
        for (Row row : store.rows(conv)) {
            r.printf("   seq=%d epoch=%d body=%s%n", row.seq(), row.epoch(), row.body());
        }
    }

    static Outcome outcomeOf(List<StoreWorker.Processed> ps, String body) {
        return ps.stream().filter(p -> p.record().body().equals(body)).findFirst().orElseThrow().outcome();
    }

    /** 사용자에게 성공으로 응답했지만 message 테이블에 그 내용이 없는 레코드. */
    List<ChatRecord> ackedButNotStored(EpochWriter w) throws Exception {
        Set<String> stored = store.rows(conv).stream().map(Row::body).collect(Collectors.toSet());
        return w.ackedToUser().stream().filter(rec -> !stored.contains(rec.body())).toList();
    }

    /**
     * (a)(b) 저장 워커가 교체 전에 따라잡은 경우. A의 101, 102는 교체 전에 저장되고,
     * 교체 뒤 A가 깨어나 보낸 103은 로그에는 남지만 저장에서 걸러진다.
     */
    @Test
    void a_b_workerCaughtUp_zombieRecordFiltered() throws Exception {
        try (var worker = new StoreWorker(KAFKA.getBootstrapServers(), topic, ds, FenceMode.LEASE_EQ);
             var a = acquire("A", 101)) {
            a.accept("A-101");
            a.accept("A-102");
            var first = worker.drainToEnd(Duration.ofSeconds(20));
            print("교체 전 처리", first);

            // A 멈춤. lease 만료, B 획득. B는 저장소의 마지막 seq에서 이어 간다.
            leases.forceExpire(conv);
            long last = store.maxSeq(conv);
            try (var b = acquire("B", store.maxSeq(conv) + 1)) {
                r.line("B가 본 마지막 seq=" + last);
                b.accept("B-103");
                b.accept("B-104");
                // A 깨어남. 자기 lease가 끝난 것을 모르고 다음 순번 103을 보낸다. 브로커는 받아 준다.
                ChatRecord zombie = a.accept("A-103");
                r.line("A의 좀비 전송 결과: 브로커 ack, 사용자에게 성공 응답 " + zombie);

                var second = worker.drainToEnd(Duration.ofSeconds(20));
                print("교체 뒤 처리", second);
                printRows();

                assertThat(outcomeOf(first, "A-101")).isEqualTo(Outcome.STORED);
                assertThat(outcomeOf(first, "A-102")).isEqualTo(Outcome.STORED);
                assertThat(outcomeOf(second, "B-103")).isEqualTo(Outcome.STORED);
                assertThat(outcomeOf(second, "B-104")).isEqualTo(Outcome.STORED);
                assertThat(outcomeOf(second, "A-103")).isEqualTo(Outcome.REJECTED_EPOCH);
                assertThat(store.rows(conv)).extracting(Row::body)
                        .containsExactly("seed-100", "A-101", "A-102", "B-103", "B-104");
                // 로그에는 A의 좀비 레코드가 그대로 남는다.
                assertThat(LogReader.readAll(KAFKA.getBootstrapServers(), topic)).extracting(ChatRecord::body)
                        .containsExactly("A-101", "A-102", "B-103", "B-104", "A-103");
                // (d) A는 브로커 ack만 보고 사용자에게 성공으로 응답했는데 저장되지 않았다.
                assertThat(ackedButNotStored(a)).extracting(ChatRecord::body).containsExactly("A-103");
            }
        }
    }

    /**
     * (b) 저장 워커가 밀려 있는 경우, PLAN의 조건(lease 테이블의 현재 epoch와 같아야 함)은
     * A가 유효한 lease로 쓴 101, 102까지 교체 뒤에 처리하면서 거부한다. B는 저장소를 보고 101부터 다시 매긴다.
     */
    @Test
    void b_workerLagging_leaseEq_dbReplay_dropsPreTakeoverRecords() throws Exception {
        try (var a = acquire("A", 101)) {
            a.accept("A-101");
            a.accept("A-102");
            // 워커는 아직 이 레코드들을 처리하지 않았다(소비 지연).
            leases.forceExpire(conv);
            long last = store.maxSeq(conv);
            r.line("B가 저장소에서 본 마지막 seq=" + last);
            try (var b = acquire("B", last + 1);
                 var worker = new StoreWorker(KAFKA.getBootstrapServers(), topic, ds, FenceMode.LEASE_EQ)) {
                b.accept("B-101");
                b.accept("B-102");
                var ps = worker.drainToEnd(Duration.ofSeconds(20));
                print("지연된 워커의 처리", ps);
                printRows();

                assertThat(last).isEqualTo(100);
                assertThat(outcomeOf(ps, "A-101")).isEqualTo(Outcome.REJECTED_EPOCH);
                assertThat(outcomeOf(ps, "A-102")).isEqualTo(Outcome.REJECTED_EPOCH);
                assertThat(store.rows(conv)).extracting(Row::body).containsExactly("seed-100", "B-101", "B-102");
                // A는 lease가 유효할 때 썼고 브로커 ack를 받아 성공으로 응답했지만 둘 다 저장되지 않았다.
                r.line("A가 성공 응답했으나 저장되지 않은 것: " + ackedButNotStored(a));
                assertThat(ackedButNotStored(a)).extracting(ChatRecord::body).containsExactly("A-101", "A-102");
            }
        }
    }

    /**
     * (b) 같은 지연 상황에서 B가 저장소가 아니라 로그를 replay해 마지막 seq를 정하면,
     * B는 102 다음인 103부터 매기고 A의 101, 102는 거부되어 101, 102 자리가 빈다.
     */
    @Test
    void b_workerLagging_leaseEq_logReplay_leavesHole() throws Exception {
        try (var a = acquire("A", 101)) {
            a.accept("A-101");
            a.accept("A-102");
            leases.forceExpire(conv);
            long lastInLog = LogReader.readAll(KAFKA.getBootstrapServers(), topic).stream()
                    .mapToLong(ChatRecord::seq).max().orElse(100);
            r.line("B가 로그 replay로 본 마지막 seq=" + lastInLog);
            try (var b = acquire("B", lastInLog + 1);
                 var worker = new StoreWorker(KAFKA.getBootstrapServers(), topic, ds, FenceMode.LEASE_EQ)) {
                b.accept("B-103");
                var ps = worker.drainToEnd(Duration.ofSeconds(20));
                print("지연된 워커의 처리", ps);
                printRows();

                assertThat(lastInLog).isEqualTo(102);
                assertThat(store.rows(conv)).extracting(Row::seq).containsExactly(100L, 103L);
            }
        }
    }

    /**
     * (b) 변형: 저장소가 lease 테이블 대신 "지금까지 처리한 가장 큰 epoch"와 비교하고(LOG_ORDER),
     * 새 담당자는 lease 획득 뒤 로그에 교체 표시 레코드를 쓰고 워커가 그것을 처리한 뒤에 저장소의 마지막 seq를 읽는다.
     * 로그에서 표시보다 앞에 있는 A의 레코드는 교체 뒤에 처리해도 저장되고, 표시 뒤의 A 레코드만 거부된다.
     */
    @Test
    void b_workerLagging_logOrder_keepsRecordsBeforeMarker() throws Exception {
        try (var a = acquire("A", 101)) {
            a.accept("A-101");
            a.accept("A-102");
            leases.forceExpire(conv);
            long epochB = leases.tryAcquire(conv, "B", Duration.ofSeconds(30)).orElseThrow();
            // lease는 넘어갔지만 A는 모른다. B가 표시를 쓰기 전에 A의 전송이 로그에 먼저 들어간 경우.
            a.accept("A-103");
            try (var bProducer = new EpochWriter("B", KAFKA.getBootstrapServers(), topic, conv, epochB, -1);
                 var worker = new StoreWorker(KAFKA.getBootstrapServers(), topic, ds, FenceMode.LOG_ORDER)) {
                long markerOffset = bProducer.writeMarker();
                // B는 워커가 표시까지 처리한 것을 확인한 뒤에 저장소의 마지막 seq를 읽는다.
                var ps1 = worker.drainToEnd(Duration.ofSeconds(20));
                print("표시까지 처리", ps1);
                long last = store.maxSeq(conv);
                r.line("표시 offset=" + markerOffset + ", B가 저장소에서 본 마지막 seq=" + last);
                try (var b = new EpochWriter("B", KAFKA.getBootstrapServers(), topic, conv, epochB, last + 1)) {
                    b.accept("B-104");
                    a.accept("A-104"); // 표시 뒤의 좀비 전송
                    var ps2 = worker.drainToEnd(Duration.ofSeconds(20));
                    print("표시 뒤 처리", ps2);
                    printRows();

                    assertThat(last).isEqualTo(103);
                    assertThat(outcomeOf(ps1, "A-101")).isEqualTo(Outcome.STORED);
                    assertThat(outcomeOf(ps1, "A-103")).isEqualTo(Outcome.STORED);
                    assertThat(outcomeOf(ps2, "A-104")).isEqualTo(Outcome.REJECTED_EPOCH);
                    assertThat(store.rows(conv)).extracting(Row::body)
                            .containsExactly("seed-100", "A-101", "A-102", "A-103", "B-104");
                    // (d)는 이 변형에서도 남는다. 표시 뒤의 A-104는 브로커 ack로 성공 응답되었지만 저장되지 않았다.
                    r.line("A가 성공 응답했으나 저장되지 않은 것: " + ackedButNotStored(a));
                    assertThat(ackedButNotStored(a)).extracting(ChatRecord::body).containsExactly("A-104");
                }
            }
        }
    }

    /**
     * (d) 옛 담당자가 로그 기록 성공(브로커 ack)만 보고 사용자에게 성공으로 응답하는 경로.
     * 여러 번 보내도 브로커는 모두 받아 주고, 저장은 모두 거부한다. A는 어느 쪽 결과도 통보받지 않는다.
     */
    @Test
    void d_zombieAckedByBrokerButRejectedByStore() throws Exception {
        try (var worker = new StoreWorker(KAFKA.getBootstrapServers(), topic, ds, FenceMode.LEASE_EQ);
             var a = acquire("A", 101)) {
            a.accept("A-101");
            worker.drainToEnd(Duration.ofSeconds(20));
            leases.forceExpire(conv);
            try (var b = acquire("B", store.maxSeq(conv) + 1)) {
                b.accept("B-102");
                // A 깨어남. 세 번 보낸다. 모두 예외 없이 브로커 ack를 받는다.
                a.accept("A-102");
                a.accept("A-103");
                a.accept("A-104");
                b.accept("B-103");
                var ps = worker.drainToEnd(Duration.ofSeconds(20));
                print("처리", ps);
                printRows();
                var lost = ackedButNotStored(a);
                r.line("A가 사용자에게 성공으로 응답한 것: " + a.ackedToUser().size()
                        + "건, 그중 저장되지 않은 것: " + lost);
                assertThat(lost).extracting(ChatRecord::body).containsExactly("A-102", "A-103", "A-104");
                assertThat(ps.stream().filter(p -> p.record().epoch() == a.epoch()).map(StoreWorker.Processed::outcome))
                        .containsOnly(Outcome.REJECTED_EPOCH);
                assertThat(ackedButNotStored(b)).isEmpty();
            }
        }
    }
}
