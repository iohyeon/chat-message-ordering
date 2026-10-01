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
     * 로그를 순서대로 처리하는 워커 하나가 쓰는 것을 전제로 한다. 교체 표시뿐 아니라 모든 레코드가 fence 행을
     * {@code greatest(max_epoch, 레코드 epoch)} 로 UPDATE한다. 값이 같아도 새 행 버전을 만들고, 그 행의 배타 잠금을 커밋까지 쥔다.
     * 그래서 표시 없이 더 큰 epoch의 레코드가 먼저 오면 그 레코드가 fence를 올린다.
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
            """),

    /**
     * Q7 비교용. 저장소가 본 가장 큰 epoch(message_fence)와 비교하되, fence 행을 잠그지 않고 문장 스냅숏으로 읽는다.
     * fence는 교체 표시를 처리할 때({@link MessageStore#bumpFence})만 오른다. 잠금이 없을 때 경쟁이 생기는지 보려는 대조군이다.
     */
    LOG_ORDER_SNAPSHOT("""
            INSERT INTO message (conversation_id, conversation_seq, epoch, body)
            SELECT ?, ?, ?, ?
            WHERE ? >= (SELECT max_epoch FROM message_fence WHERE conversation_id = ?)
            ON CONFLICT (conversation_id, conversation_seq) DO NOTHING
            """),

    /**
     * Q7 비교용. fence 행을 {@code FOR SHARE} 로 읽는다. 레코드마다 fence 행에 새 버전을 쓰지 않고, 공유 잠금만 커밋까지 쥔다.
     * 공유 잠금끼리는 서로 기다리지 않고, 교체 표시의 fence 갱신(UPDATE)과만 서로 기다린다.
     */
    LOG_ORDER_FOR_SHARE("""
            INSERT INTO message (conversation_id, conversation_seq, epoch, body)
            SELECT ?, ?, ?, ?
            WHERE ? >= (SELECT max_epoch FROM message_fence WHERE conversation_id = ? FOR SHARE)
            ON CONFLICT (conversation_id, conversation_seq) DO NOTHING
            """),

    /**
     * Q7 비교용. fence 행을 {@code FOR UPDATE} 로 읽는다. LOG_ORDER처럼 배타 잠금을 커밋까지 쥐지만 fence 행에 새 버전을 만들지 않는다.
     * LOG_ORDER의 처리량 하락이 "레코드마다 쓰는 것" 때문인지 "배타 잠금" 때문인지 나누려는 것이다.
     */
    LOG_ORDER_FOR_UPDATE("""
            INSERT INTO message (conversation_id, conversation_seq, epoch, body)
            SELECT ?, ?, ?, ?
            WHERE ? >= (SELECT max_epoch FROM message_fence WHERE conversation_id = ? FOR UPDATE)
            ON CONFLICT (conversation_id, conversation_seq) DO NOTHING
            """);

    final String sql;

    FenceMode(String sql) {
        this.sql = sql;
    }

    public String sql() {
        return sql;
    }

    /** message_fence와 비교하는 방식인가. 이 방식들은 교체 표시를 만나면 fence를 올린다. */
    public boolean usesFence() {
        return this == LOG_ORDER || this == LOG_ORDER_SNAPSHOT || this == LOG_ORDER_FOR_SHARE
                || this == LOG_ORDER_FOR_UPDATE;
    }
}
