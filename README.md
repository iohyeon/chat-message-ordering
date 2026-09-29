# kafka-fencing-lab

채팅 메시지의 순번을 매기는 담당자가 바뀔 때, 옛 담당자(zombie writer)의 쓰기를 **Kafka 브로커에서 막을지, 저장소에서 막을지**를 직접 재현해 비교하는 실험 저장소입니다. Kafka KRaft 단일 브로커와 PostgreSQL 16을 Testcontainers로 띄우고, 시나리오 6개를 JUnit 테스트와 측정 코드로 만듭니다.

확인하려는 질문은 다음과 같습니다.

- 멱등 producer만 쓰면 옛 담당자의 쓰기가 막히는가
- 트랜잭션 producer에서 옛 epoch의 요청은 `send()` 시점에 거부되는가, 커밋 시점에 거부되는가
- 중단(abort)된 트랜잭션의 레코드는 `read_uncommitted` 소비자에게 보이는가
- 저장소의 조건부 쓰기(fencing token)는 브로커 fencing과 비교해 비용이 얼마인가

## Background

대화마다 담당자 하나가 `conversation_seq` 를 매기고 Kafka에는 기록만 하는 구조를 가정합니다. 담당자가 GC 멈춤 등으로 lease가 끝난 줄 모르고 깨어나면, 새 담당자와 같은 순번을 쓸 수 있습니다.

```text
t0  옛 담당자 A(epoch 5): begin, send(seq 101), send(seq 102), 멈춤
t1  lease 만료. 새 담당자 B가 lease 획득
t2  B: initTransactions() → epoch 6. A의 열린 트랜잭션은 abort 마커로 닫힘
t3  B: 로그 replay로 마지막 커밋 seq 확인 → 100
t4  B: seq 101부터 수락
t5  A 깨어남: send(seq 103), commitTransaction() → 거부
```

이 흐름이 맞다면 두 가지가 따라옵니다. 브로커에서 막으려면 트랜잭션 producer가 필요하고, t2에서 abort된 레코드는 로그에 남으므로 소비자도 `read_committed` 로 읽어야 합니다. 저장소에서 막는다면 레코드에 lease epoch를 싣고 저장할 때 검사하면 되고, producer는 멱등 producer로 충분합니다.

## Experiments

| 실험 | 확인하는 것 | 방법 | 상태 |
| --- | --- | --- | --- |
| Q1 멱등 producer | 옛 담당자의 쓰기가 성공하는가 | producer 두 개가 같은 파티션에 같은 seq를 쓰고 로그를 읽는다 | 작성 전 |
| Q2 트랜잭션 fencing | 거부가 일어나는 호출과 예외 클래스 | 같은 `transactional.id` 로 새 producer가 `initTransactions()` 한 뒤, 멈춰 둔 옛 producer의 `send()` Future와 `commitTransaction()` 결과를 각각 기록한다 | 작성 전 |
| Q3 abort 레코드 가시성 | abort된 레코드가 보이는 격리 수준 | Q2의 로그를 `read_uncommitted`, `read_committed` 소비자로 처음부터 읽는다 | 작성 전 |
| Q4 기동 순서 | replay를 `initTransactions()` 보다 먼저 하면 순번이 겹치는가 | 옛 담당자를 커밋 직전에 멈추고, 새 담당자의 replay와 `initTransactions()` 사이에서 깨운다 | 작성 전 |
| Q5 저장소 fencing | 옛 epoch 레코드가 저장에서 걸러지는가 | 멱등 producer만 쓰고, 저장 워커가 lease 테이블의 epoch와 비교하는 조건부 `INSERT` 로 쓴다 | 작성 전 |
| Q6 비용 | 두 방식의 지연과 처리량 | 아래 측정 항목 | 작성 전 |

GC 멈춤은 재현이 불안정하므로 옛 담당자는 `CountDownLatch` 로 명시적으로 멈췄다가 깨웁니다. 파티션은 하나로 둡니다.

Q5의 저장 조건은 다음과 같습니다.

```sql
INSERT INTO message (conversation_id, conversation_seq, epoch, body)
SELECT :conversation_id, :seq, :epoch, :body
WHERE :epoch = (SELECT epoch FROM conversation_owner WHERE conversation_id = :conversation_id)
ON CONFLICT (conversation_id, conversation_seq) DO NOTHING;
```

Q6에서 재는 것입니다.

| 측정 | 지표 | 변수 |
| --- | --- | --- |
| `read_committed` 전달 지연 | 전송부터 소비까지 p50, p99 | 트랜잭션 커밋 주기 10ms, 100ms, 1s |
| 트랜잭션 처리량 | 초당 커밋된 레코드 수 | 트랜잭션당 레코드 1, 10, 100개 |
| 저장소 fencing 비용 | 조건부 `INSERT` 와 단순 `INSERT` 의 초당 처리 수 | 동시 연결 1, 8, 32 |
| 담당 교체 공백 | lease 획득부터 수락 재개까지 | `initTransactions()` 포함 여부 |

커밋 주기 10ms, 100ms, 1s는 전달 p99 목표 1초에서 각각 1%, 10%, 100%를 차지하는 값입니다. 단일 브로커와 노트북 한 대에서 재는 값이므로 절대값이 아니라 두 방식의 상대 비교로만 씁니다.

## Stack

| 항목 | 선택 | 이유 |
| --- | --- | --- |
| 언어 | Java 21 | |
| 앱 구성 | Spring Boot 3 | 설정과 DataSource 구성에만 씁니다 |
| Kafka 클라이언트 | `kafka-clients` | Spring Kafka는 `initTransactions()` 호출 시점과 producer 재생성을 감춥니다. 이 실험은 그 시점을 확인하는 것이라 클라이언트를 직접 씁니다 |
| DB | PostgreSQL 16 | 조건부 `INSERT ... SELECT ... WHERE` 와 `ON CONFLICT` 로 fencing 검사를 한 문장에 씁니다 |
| 실행 환경 | Testcontainers | 시나리오마다 깨끗한 브로커와 DB를 띄웁니다 |

## Reproducing the experiments

코드를 작성하는 중입니다. 실행 명령과 결과표는 실험이 끝나면 이 절에 둡니다. 설계와 진행 순서는 [PLAN.md](PLAN.md)에 있습니다.

colima에서 Testcontainers를 돌릴 때는 다음 환경 변수를 씁니다.

```bash
export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```

## References

- [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)
- [Apache Kafka Design](https://kafka.apache.org/43/design/design/)
- Martin Kleppmann, [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) (2016)

## Notes

채팅 설계는 『가상 면접 사례로 배우는 대규모 시스템 설계 기초』(알렉스 쉬, 인사이트) 12장을 읽고 확장한 설계에서 출발했습니다. 책의 내용을 옮기지 않았고, 실험 설계와 코드는 Kafka 공식 문서와 KIP를 바탕으로 직접 작성했습니다.

실험 결과와 설계 과정은 [기술 블로그](https://develop-tracking.tistory.com/)에 씁니다. 틀린 부분은 이슈로 알려 주세요.
