-- Q5, Q6 저장소 fencing 실험의 스키마.
-- Spring Boot가 자동으로 실행하지 않도록 schema.sql 이 아닌 이름을 쓴다. 테스트가 직접 실행한다.

-- 담당 lease. 획득할 때 조건부 UPDATE로 epoch를 1 올린다.
CREATE TABLE IF NOT EXISTS conversation_owner (
    conversation_id text        PRIMARY KEY,
    epoch           bigint      NOT NULL,
    owner           text,
    expires_at      timestamptz NOT NULL
);

-- 저장된 메시지. (conversation_id, conversation_seq) 가 유일하므로 같은 순번은 한 번만 들어간다.
CREATE TABLE IF NOT EXISTS message (
    conversation_id  text        NOT NULL,
    conversation_seq bigint      NOT NULL,
    epoch            bigint      NOT NULL,
    body             text        NOT NULL,
    stored_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (conversation_id, conversation_seq)
);

-- 로그 순서 기준 fencing(Kleppmann 방식) 변형에서 쓴다. 저장소가 지금까지 본 가장 큰 epoch를 대화별로 기억한다.
CREATE TABLE IF NOT EXISTS message_fence (
    conversation_id text   PRIMARY KEY,
    max_epoch       bigint NOT NULL
);
