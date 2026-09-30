package lab.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/** message 테이블 접근. 쓰기는 호출자가 준 연결에서 하므로 autocommit과 명시적 트랜잭션을 둘 다 실험할 수 있다. */
public final class MessageStore {

    /** 쓰기 결과. 조건부 INSERT는 0행이라는 사실만 돌려주므로, 0행일 때의 이유는 뒤이은 조회로 붙인다. */
    public enum Outcome {
        /** 이번 호출로 들어갔다. */
        STORED,
        /** 같은 순번에 같은 epoch의 행이 이미 있다. 같은 레코드를 다시 처리한 경우다. */
        DUPLICATE,
        /** 같은 순번 자리를 다른 epoch의 행이 차지하고 있다. ON CONFLICT DO NOTHING이 조용히 버린 경우다. */
        SLOT_TAKEN,
        /** 순번 자리는 비어 있는데 들어가지 않았다. epoch 검사에서 거부된 경우다. */
        REJECTED_EPOCH
    }

    public record Row(String conversationId, long seq, long epoch, String body) {
    }

    private final DataSource dataSource;

    public MessageStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DataSource dataSource() {
        return dataSource;
    }

    /** 한 레코드를 쓴다. 돌려주는 값은 삽입된 행 수(0 또는 1). */
    public static int insert(Connection c, FenceMode mode, ChatRecord r) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(mode.sql())) {
            bind(ps, mode, r);
            return ps.executeUpdate();
        }
    }

    static void bind(PreparedStatement ps, FenceMode mode, ChatRecord r) throws SQLException {
        switch (mode) {
            case PLAIN -> {
                ps.setString(1, r.conversationId());
                ps.setLong(2, r.seq());
                ps.setLong(3, r.epoch());
                ps.setString(4, r.body());
            }
            case LEASE_EQ, LEASE_EQ_FOR_SHARE -> {
                ps.setString(1, r.conversationId());
                ps.setLong(2, r.seq());
                ps.setLong(3, r.epoch());
                ps.setString(4, r.body());
                ps.setLong(5, r.epoch());
                ps.setString(6, r.conversationId());
            }
            case LOG_ORDER -> {
                ps.setString(1, r.conversationId());
                ps.setLong(2, r.epoch());
                ps.setString(3, r.conversationId());
                ps.setLong(4, r.seq());
                ps.setLong(5, r.epoch());
                ps.setString(6, r.body());
                ps.setLong(7, r.epoch());
            }
        }
    }

    /** LOG_ORDER 변형에서 교체 표시 레코드를 처리한다. 저장소가 본 최대 epoch를 올리고 그 값을 돌려준다. */
    public static long bumpFence(Connection c, String conversationId, long epoch) throws SQLException {
        try (var ps = c.prepareStatement("""
                INSERT INTO message_fence (conversation_id, max_epoch) VALUES (?, ?)
                ON CONFLICT (conversation_id)
                DO UPDATE SET max_epoch = greatest(message_fence.max_epoch, EXCLUDED.max_epoch)
                RETURNING max_epoch
                """)) {
            ps.setString(1, conversationId);
            ps.setLong(2, epoch);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    /**
     * 0행이었던 쓰기의 이유를 붙인다. 쓰기와 같은 순간의 판단이 아니라 뒤이은 조회 결과다.
     * 순서: 같은 행이 있으면 DUPLICATE, 지금 기준 epoch와 다르면 REJECTED_EPOCH, 다른 행이 자리를 차지했으면 SLOT_TAKEN.
     * 어느 것도 아니면 설명되지 않는 0행이므로 예외를 던진다.
     */
    public static Outcome classify(Connection c, FenceMode mode, ChatRecord r, int inserted) throws SQLException {
        if (inserted == 1) {
            return Outcome.STORED;
        }
        Long existingEpoch = null;
        String existingBody = null;
        try (var ps = c.prepareStatement(
                "SELECT epoch, body FROM message WHERE conversation_id = ? AND conversation_seq = ?")) {
            ps.setString(1, r.conversationId());
            ps.setLong(2, r.seq());
            try (var rs = ps.executeQuery()) {
                if (rs.next()) {
                    existingEpoch = rs.getLong(1);
                    existingBody = rs.getString(2);
                }
            }
        }
        if (existingEpoch != null && existingEpoch == r.epoch() && existingBody.equals(r.body())) {
            return Outcome.DUPLICATE;
        }
        Long reference = switch (mode) {
            case PLAIN -> null;
            case LEASE_EQ, LEASE_EQ_FOR_SHARE ->
                    scalar(c, "SELECT epoch FROM conversation_owner WHERE conversation_id = ?", r.conversationId());
            case LOG_ORDER ->
                    scalar(c, "SELECT max_epoch FROM message_fence WHERE conversation_id = ?", r.conversationId());
        };
        if (reference != null && reference != r.epoch()) {
            return Outcome.REJECTED_EPOCH;
        }
        if (existingEpoch != null) {
            return Outcome.SLOT_TAKEN;
        }
        throw new IllegalStateException("설명되지 않는 0행: " + r + " mode=" + mode + " reference=" + reference);
    }

    private static Long scalar(Connection c, String sql, String conversationId) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            ps.setString(1, conversationId);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : null;
            }
        }
    }

    public long maxSeq(String conversationId) throws SQLException {
        try (Connection c = dataSource.getConnection()) {
            return maxSeq(c, conversationId);
        }
    }

    public static long maxSeq(Connection c, String conversationId) throws SQLException {
        try (var ps = c.prepareStatement(
                "SELECT coalesce(max(conversation_seq), 0) FROM message WHERE conversation_id = ?")) {
            ps.setString(1, conversationId);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    public List<Row> rows(String conversationId) throws SQLException {
        try (Connection c = dataSource.getConnection();
             var ps = c.prepareStatement("""
                     SELECT conversation_id, conversation_seq, epoch, body FROM message
                      WHERE conversation_id = ? ORDER BY conversation_seq
                     """)) {
            ps.setString(1, conversationId);
            List<Row> out = new ArrayList<>();
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Row(rs.getString(1), rs.getLong(2), rs.getLong(3), rs.getString(4)));
                }
            }
            return out;
        }
    }
}
