package lab.lease;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.OptionalLong;
import javax.sql.DataSource;

/**
 * PostgreSQL lease 테이블 {@code conversation_owner}.
 *
 * <p>획득은 조건부 UPDATE 한 문장이다. 만료된 lease만 가져올 수 있고, 가져오면 epoch가 1 오른다.
 * 두 담당자가 동시에 획득하려 하면 행 잠금 때문에 한쪽 UPDATE가 기다리고, READ COMMITTED에서
 * 기다린 쪽은 새 행 버전으로 {@code expires_at <= now()} 를 다시 평가하므로 실패한다.
 */
public final class LeaseRepository {

    static final String ACQUIRE = """
            UPDATE conversation_owner
               SET epoch = epoch + 1,
                   owner = ?,
                   expires_at = now() + (? * interval '1 millisecond')
             WHERE conversation_id = ?
               AND expires_at <= now()
            RETURNING epoch
            """;

    private final DataSource dataSource;

    public LeaseRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** 대화의 lease 행을 만든다. 처음에는 만료된 상태로 두어 바로 획득할 수 있게 한다. */
    public void create(String conversationId, long initialEpoch) throws SQLException {
        try (Connection c = dataSource.getConnection();
             var ps = c.prepareStatement("""
                     INSERT INTO conversation_owner (conversation_id, epoch, owner, expires_at)
                     VALUES (?, ?, NULL, now() - interval '1 second')
                     ON CONFLICT (conversation_id) DO NOTHING
                     """)) {
            ps.setString(1, conversationId);
            ps.setLong(2, initialEpoch);
            ps.executeUpdate();
        }
    }

    /** lease를 획득한다. 성공하면 새 epoch, 아직 다른 담당자의 lease가 유효하면 빈 값. autocommit으로 실행한다. */
    public OptionalLong tryAcquire(String conversationId, String owner, Duration ttl) throws SQLException {
        try (Connection c = dataSource.getConnection()) {
            return tryAcquire(c, conversationId, owner, ttl);
        }
    }

    /** 호출자가 준 연결(트랜잭션)에서 lease를 획득한다. 커밋은 호출자가 한다. */
    public static OptionalLong tryAcquire(Connection c, String conversationId, String owner, Duration ttl)
            throws SQLException {
        try (var ps = c.prepareStatement(ACQUIRE)) {
            ps.setString(1, owner);
            ps.setLong(2, ttl.toMillis());
            ps.setString(3, conversationId);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? OptionalLong.of(rs.getLong(1)) : OptionalLong.empty();
            }
        }
    }

    /**
     * 실험용. lease의 TTL이 지난 상태를 만든다. GC 멈춤처럼 시간이 흐른 것을 기다리지 않고 흉내 낸다.
     * 담당자는 이 사실을 통보받지 않는다.
     */
    public void forceExpire(String conversationId) throws SQLException {
        try (Connection c = dataSource.getConnection();
             var ps = c.prepareStatement(
                     "UPDATE conversation_owner SET expires_at = now() - interval '1 second' WHERE conversation_id = ?")) {
            ps.setString(1, conversationId);
            ps.executeUpdate();
        }
    }

    /**
     * Q9. 이 epoch의 lease가 아직 유효한가. epoch가 같고 만료 시각이 지나지 않았으면 참이다.
     * 한 시점의 확인일 뿐이므로, 돌려준 직후에 만료될 수 있다.
     */
    public boolean isOwner(String conversationId, long epoch) throws SQLException {
        try (Connection c = dataSource.getConnection();
             var ps = c.prepareStatement(
                     "SELECT 1 FROM conversation_owner WHERE conversation_id = ? AND epoch = ? AND expires_at > now()")) {
            ps.setString(1, conversationId);
            ps.setLong(2, epoch);
            try (var rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    /** Q9 실험용. 이 epoch의 lease가 지금부터 {@code ttl} 뒤에 만료되게 한다. 담당자는 통보받지 않는다. */
    public void expireIn(String conversationId, long epoch, Duration ttl) throws SQLException {
        try (Connection c = dataSource.getConnection();
             var ps = c.prepareStatement("""
                     UPDATE conversation_owner SET expires_at = now() + (? * interval '1 millisecond')
                      WHERE conversation_id = ? AND epoch = ?
                     """)) {
            ps.setLong(1, ttl.toMillis());
            ps.setString(2, conversationId);
            ps.setLong(3, epoch);
            ps.executeUpdate();
        }
    }

    /** Q9. 이 epoch의 lease를 스스로 놓는다(곧바로 만료). 이미 다른 epoch로 넘어갔으면 아무것도 바꾸지 않는다. */
    public boolean release(String conversationId, long epoch) throws SQLException {
        try (Connection c = dataSource.getConnection();
             var ps = c.prepareStatement("""
                     UPDATE conversation_owner SET expires_at = now() - interval '1 millisecond'
                      WHERE conversation_id = ? AND epoch = ? AND expires_at > now()
                     """)) {
            ps.setString(1, conversationId);
            ps.setLong(2, epoch);
            return ps.executeUpdate() == 1;
        }
    }

    public long currentEpoch(String conversationId) throws SQLException {
        try (Connection c = dataSource.getConnection();
             var ps = c.prepareStatement("SELECT epoch FROM conversation_owner WHERE conversation_id = ?")) {
            ps.setString(1, conversationId);
            try (var rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("lease 행이 없다: " + conversationId);
                }
                return rs.getLong(1);
            }
        }
    }
}
