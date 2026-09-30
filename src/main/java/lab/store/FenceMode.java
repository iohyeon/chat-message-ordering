package lab.store;

/** 저장 워커가 message 테이블에 쓰는 방식. */
public enum FenceMode {

    /** 검사 없이 넣는다. 비교 기준. */
    PLAIN("""
            INSERT INTO message (conversation_id, conversation_seq, epoch, body)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (conversation_id, conversation_seq) DO NOTHING
            """),

    /** PLAN.md의 조건부 INSERT 그대로. lease 행을 잠그지 않고 문장 시작 시점의 스냅숏으로 epoch를 읽는다. */
    LEASE_EQ("""
            INSERT INTO message (conversation_id, conversation_seq, epoch, body)
            SELECT ?, ?, ?, ?
            WHERE ? = (SELECT epoch FROM conversation_owner WHERE conversation_id = ?)
            ON CONFLICT (conversation_id, conversation_seq) DO NOTHING
            """),

    /**
     * LEASE_EQ의 서브쿼리에 {@code FOR SHARE} 를 붙인 것. lease 행에 공유 잠금을 걸어 커밋까지 쥔다.
     * 그동안 lease 교체 UPDATE는 기다리고, 먼저 잡힌 UPDATE가 있으면 이 문장이 기다린 뒤 최신 행을 다시 읽는다.
     */
    LEASE_EQ_FOR_SHARE("""
            INSERT INTO message (conversation_id, conversation_seq, epoch, body)
            SELECT ?, ?, ?, ?
            WHERE ? = (SELECT epoch FROM conversation_owner WHERE conversation_id = ? FOR SHARE)
            ON CONFLICT (conversation_id, conversation_seq) DO NOTHING
            """),

    /**
     * lease 테이블이 아니라 저장소가 지금까지 본 가장 큰 epoch(message_fence)와 비교한다(Kleppmann의 fencing token).
     * 로그를 순서대로 처리하는 워커 하나가 쓰는 것을 전제로 한다. fence 행은 ON CONFLICT DO UPDATE로 잠겨 커밋까지 유지된다.
     */
    LOG_ORDER("""
            WITH f AS (
                INSERT INTO message_fence (conversation_id, max_epoch) VALUES (?, ?)
                ON CONFLICT (conversation_id)
                DO UPDATE SET max_epoch = greatest(message_fence.max_epoch, EXCLUDED.max_epoch)
                RETURNING max_epoch
            )
            INSERT INTO message (conversation_id, conversation_seq, epoch, body)
            SELECT ?, ?, ?, ? FROM f WHERE f.max_epoch = ?
            ON CONFLICT (conversation_id, conversation_seq) DO NOTHING
            """);

    final String sql;

    FenceMode(String sql) {
        this.sql = sql;
    }

    public String sql() {
        return sql;
    }
}
