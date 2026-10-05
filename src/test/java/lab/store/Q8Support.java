package lab.store;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Q8 준비 코드. 같은 로그를 비교 방식과 밀림 여부가 다른 저장 워커 여러 개가 동시에 처리하게 한다.
 *
 * <p>워커마다 PostgreSQL 스키마 하나를 주고, 그 스키마에 {@code message} 와 {@code message_fence} 를 따로 만든다.
 * lease는 하나여야 하므로 {@code conversation_owner} 는 public의 테이블 하나를 가리키는 뷰로 둔다. 워커의 연결은
 * {@code search_path} 를 자기 스키마로 두므로 {@link FenceMode} 의 SQL을 고치지 않고 그대로 쓴다.
 * 뷰에 붙인 {@code FOR SHARE} 는 뷰가 읽는 테이블의 행을 잠근다(Q8 테스트에서 따로 확인한다).
 */
public final class Q8Support {

    private Q8Support() {
    }

    /** 저장 워커 하나의 이름. 비교 방식과, 교체 전에 따라잡았는지(caught) 밀렸는지(lagging). */
    public record WorkerKey(FenceMode mode, boolean lagging) {
        public String schema() {
            return "q8_" + mode.name().toLowerCase(Locale.ROOT) + (lagging ? "_lag" : "_cur");
        }

        public String label() {
            return mode + (lagging ? "/밀림" : "/따라잡음");
        }

        public String csvLabel() {
            return mode + (lagging ? "/lagging" : "/caught-up");
        }
    }

    public static final List<FenceMode> MODES = List.of(FenceMode.LEASE_EQ, FenceMode.LEASE_EQ_FOR_SHARE,
            FenceMode.LOG_ORDER, FenceMode.PLAIN);

    public static List<WorkerKey> keys() {
        List<WorkerKey> out = new ArrayList<>();
        for (boolean lag : new boolean[] {false, true}) {
            for (FenceMode m : MODES) {
                out.add(new WorkerKey(m, lag));
            }
        }
        return out;
    }

    /** public에 원래 스키마를 만들고, 워커마다 스키마를 만든다. 이미 있으면 지우고 새로 만든다. */
    public static void createSchemas(DataSource publicDs, List<WorkerKey> keys) throws Exception {
        StoreFixture.applySchema(publicDs);
        try (Connection c = publicDs.getConnection(); var st = c.createStatement()) {
            for (WorkerKey k : keys) {
                String s = k.schema();
                st.execute("DROP SCHEMA IF EXISTS " + s + " CASCADE");
                st.execute("CREATE SCHEMA " + s);
                st.execute("CREATE TABLE " + s + ".message (LIKE public.message INCLUDING ALL)");
                st.execute("CREATE TABLE " + s + ".message_fence (LIKE public.message_fence INCLUDING ALL)");
                st.execute("CREATE VIEW " + s + ".conversation_owner AS SELECT * FROM public.conversation_owner");
            }
        }
    }

    public static HikariDataSource dataSource(PostgreSQLContainer pg, String schema, int poolSize) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(pg.getJdbcUrl());
        cfg.setUsername(pg.getUsername());
        cfg.setPassword(pg.getPassword());
        cfg.setMaximumPoolSize(poolSize);
        cfg.setMinimumIdle(poolSize);
        cfg.setAutoCommit(true);
        cfg.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        if (schema != null) {
            cfg.setConnectionInitSql("SET search_path TO " + schema);
        }
        return new HikariDataSource(cfg);
    }

    /** 격리 수준별 끝 offset. read_uncommitted는 HW, read_committed는 LSO. */
    public static long endOffset(Admin admin, String topic, IsolationLevel isolation) throws Exception {
        TopicPartition tp = new TopicPartition(topic, 0);
        return admin.listOffsets(Map.of(tp, OffsetSpec.latest()), new ListOffsetsOptions(isolation))
                .partitionResult(tp).get(30, TimeUnit.SECONDS).offset();
    }

    /**
     * LSO가 HW를 따라잡을 때까지 기다린다. 트랜잭션 버전 2에서 {@code commitTransaction()} 은 조정자가 PREPARE_COMMIT을
     * 기록한 뒤 돌아오고 마커는 그 뒤에 쓰이므로, 커밋 직후에는 read_committed 소비자에게 아직 안 보일 수 있다.
     * 돌려주는 값은 {HW, LSO}.
     */
    public static long[] awaitStable(Admin admin, String topic) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (true) {
            long hw = endOffset(admin, topic, IsolationLevel.READ_UNCOMMITTED);
            long lso = endOffset(admin, topic, IsolationLevel.READ_COMMITTED);
            if (lso >= hw || System.nanoTime() > deadline) {
                return new long[] {hw, lso};
            }
            Thread.sleep(20);
        }
    }

    public static Admin admin(String bootstrap) {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
    }

    /** 한 워커 스키마의 message 테이블. 순번에서 행으로. */
    public static Map<Long, MessageStore.Row> rows(DataSource ds, String conversationId) throws SQLException {
        Map<Long, MessageStore.Row> out = new java.util.TreeMap<>();
        for (MessageStore.Row r : new MessageStore(ds).rows(conversationId)) {
            out.put(r.seq(), r);
        }
        return out;
    }

    /** message 테이블에서 가장 작은 순번과 가장 큰 순번 사이에 빈 순번. */
    public static List<Long> gaps(Map<Long, MessageStore.Row> rows) {
        return rows.isEmpty() ? new ArrayList<>() : gaps(rows, rows.keySet().iterator().next());
    }

    /** {@code from} 부터 가장 큰 순번 사이에 빈 순번. 대화의 첫 순번을 알 때 쓴다(맨 앞이 빈 경우까지 센다). */
    public static List<Long> gaps(Map<Long, MessageStore.Row> rows, long from) {
        List<Long> out = new ArrayList<>();
        if (rows.isEmpty()) {
            return out;
        }
        TreeSet<Long> seqs = new TreeSet<>(rows.keySet());
        for (long s = from; s <= seqs.last(); s++) {
            if (!seqs.contains(s)) {
                out.add(s);
            }
        }
        return out;
    }

    /** 성공 응답한 레코드 중 message 테이블에 그 본문이 없는 것. */
    public static List<ChatRecord> ackedButNotStored(List<ChatRecord> acked, Map<Long, MessageStore.Row> rows) {
        Set<String> stored = rows.values().stream().map(MessageStore.Row::body).collect(Collectors.toSet());
        return acked.stream().filter(r -> !stored.contains(r.body())).toList();
    }

    /** 실패 응답한 레코드 중 message 테이블에 들어간 것. */
    public static List<ChatRecord> failedButStored(List<ChatRecord> failed, Map<Long, MessageStore.Row> rows) {
        Set<String> stored = rows.values().stream().map(MessageStore.Row::body).collect(Collectors.toSet());
        return failed.stream().filter(r -> stored.contains(r.body())).toList();
    }

    /** read_committed 로그에서 같은 순번이 두 번 이상 나온 순번과 그 본문들. 교체 표시는 뺀다. */
    public static Map<Long, List<String>> duplicateSeqsInLog(List<ChatRecord> log) {
        Map<Long, List<String>> bySeq = new HashMap<>();
        for (ChatRecord r : log) {
            if (!r.marker()) {
                bySeq.computeIfAbsent(r.seq(), x -> new ArrayList<>()).add(r.body());
            }
        }
        Map<Long, List<String>> out = new java.util.TreeMap<>();
        bySeq.forEach((k, v) -> {
            if (v.size() > 1) {
                out.put(k, v);
            }
        });
        return out;
    }
}
