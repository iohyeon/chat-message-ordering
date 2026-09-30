package lab.store;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Q5, Q6가 함께 쓰는 준비 코드. 스키마 적용, 테이블 비우기, 토픽 생성. */
public final class StoreFixture {

    private StoreFixture() {
    }

    public static HikariDataSource dataSource(PostgreSQLContainer pg, int poolSize) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(pg.getJdbcUrl());
        cfg.setUsername(pg.getUsername());
        cfg.setPassword(pg.getPassword());
        cfg.setMaximumPoolSize(poolSize);
        cfg.setMinimumIdle(poolSize);
        cfg.setAutoCommit(true);
        // 기본 격리 수준(READ COMMITTED)을 쓴다는 것을 드러내려고 적는다.
        cfg.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        return new HikariDataSource(cfg);
    }

    public static void applySchema(javax.sql.DataSource ds) throws SQLException, IOException {
        String sql;
        try (InputStream in = StoreFixture.class.getResourceAsStream("/store-schema.sql")) {
            sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        try (Connection c = ds.getConnection(); var st = c.createStatement()) {
            st.execute(sql);
        }
    }

    public static void truncate(javax.sql.DataSource ds) throws SQLException {
        try (Connection c = ds.getConnection(); var st = c.createStatement()) {
            st.execute("TRUNCATE message, conversation_owner, message_fence");
        }
    }

    public static void createTopic(String bootstrap, String topic) throws Exception {
        createTopic(bootstrap, topic, Map.of());
    }

    public static void createTopic(String bootstrap, String topic, Map<String, String> configs) throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.createTopics(List.of(new NewTopic(topic, Optional.of(1), Optional.empty()).configs(configs))).all().get();
        }
    }

    public static void seed(javax.sql.DataSource ds, String conversationId, long seq, long epoch) throws SQLException {
        try (Connection c = ds.getConnection()) {
            MessageStore.insert(c, FenceMode.PLAIN, ChatRecord.message(conversationId, seq, epoch, "seed-" + seq));
        }
    }
}
