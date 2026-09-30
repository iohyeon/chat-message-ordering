# 실험 계획

README의 실험 6개를 어떤 순서와 구조로 만드는지 적은 문서입니다.

## 상태

완료. 실험 6개의 결과는 다음 문서에 있습니다. 아래 계획은 작업 전에 쓴 것이고, 실제 테스트와 측정 클래스 이름과 구성은 결과 문서의 "실행 명령" 절을 따릅니다.

| 실험 | 결과 문서 |
| --- | --- |
| Q1 멱등 producer | [results/Q1.md](results/Q1.md) |
| Q2 트랜잭션 fencing | [results/Q2.md](results/Q2.md) |
| Q3 abort 레코드 가시성 | [results/Q3.md](results/Q3.md) |
| Q4 기동 순서 | [results/Q4.md](results/Q4.md) |
| Q5 저장소 fencing | [results/Q5.md](results/Q5.md) |
| Q6 비용 | [results/Q6.md](results/Q6.md) |

## 확인할 질문과 지금의 예상

| 실험 | 질문 | 예상 | 예상의 근거 |
| --- | --- | --- | --- |
| Q1 | 멱등 producer만 쓰면 옛 담당자가 막히는가 | 막히지 않는다 | 멱등 producer는 프로세스마다 새 producer ID를 받아, 브로커가 보기에 두 담당자는 무관한 producer다 |
| Q2 | 옛 epoch의 거부는 `send()` 시점인가, 커밋 시점인가 | 확인 필요 | KIP-98은 트랜잭션 시작, 커밋, 중단 호출에서 `ProducerFencedException` 이 난다고 적는다 |
| Q3 | abort된 레코드는 `read_uncommitted` 소비자에게 보이는가 | 보인다 | abort된 레코드는 로그에서 지워지지 않고 abort 마커만 붙는다 |
| Q4 | replay를 `initTransactions()` 보다 먼저 하면 순번이 겹치는가 | 겹친다 | 그 사이에 옛 담당자가 커밋을 끝내면 새 담당자는 그 레코드를 모른다 |
| Q5 | 저장소 조건부 쓰기로 옛 담당자의 레코드가 걸러지는가 | 걸러진다 | 저장 워커가 lease 테이블의 현재 epoch와 비교한다 |
| Q6 | 두 방식의 비용은 얼마인가 | 측정 전 | |

## 코드 구조

```text
src/main/java/lab/
  shard/    담당자 흉내: seq 부여, producer 보유
  lease/    PostgreSQL lease 테이블 (조건부 UPDATE로 획득하고 epoch를 올린다)
  store/    저장 워커: 로그를 읽어 message 테이블에 쓴다
  zombie/   옛 담당자를 멈췄다 깨우는 도구
src/test/java/lab/
  Q1IdempotentProducerTest
  Q2TransactionalFencingTest
  Q3AbortedRecordVisibilityTest
  Q4StartupOrderTest
  Q5StoreFencingTest
  Q6CostBenchmark          테스트가 아니라 측정. 따로 실행
```

## 시나리오 절차

모든 시나리오는 토픽 `chat-log` 파티션 1개, 대화 1개로 시작합니다.

**Q1.** 담당자 A가 멱등 producer로 seq 101, 102를 보내고 멈춥니다. 담당자 B가 새 멱등 producer로 seq 101, 102를 보냅니다. A가 깨어나 seq 103을 보냅니다. 로그에 다섯 레코드가 모두 있고 A의 전송이 예외 없이 끝나는지 확인합니다.

**Q2.** A가 `transactional.id=conv-shard-1` 로 `initTransactions()`, `beginTransaction()` 을 하고 seq 101을 보낸 뒤 커밋 전에 멈춥니다. B가 같은 id로 `initTransactions()` 를 부릅니다. A가 깨어나 seq 102를 보내고 `Future.get()` 결과를 기록한 뒤 `commitTransaction()` 결과를 기록합니다. 예외가 어느 호출에서 나는지와 예외 클래스를 확인합니다.

**Q3.** Q2의 로그를 `read_uncommitted`, `read_committed` 소비자로 처음부터 읽고, A의 seq 101이 각각에서 보이는지 확인합니다.

**Q4.** A가 트랜잭션으로 seq 101, 102를 보내고 커밋 직전에 멈춥니다. B가 replay로 마지막 커밋 seq(100)를 확인하고, `initTransactions()` 전에 A를 깨워 커밋시킵니다. B가 seq 101부터 수락합니다. `read_committed` 소비자에게 seq 101이 두 번 보이는지 확인하고, 순서를 바로잡아(`initTransactions()` 먼저) 한 번만 보이는지도 확인합니다.

**Q5.** 트랜잭션 없이 멱등 producer만 쓰고, 레코드에 lease epoch를 싣습니다. lease가 A(epoch 5)에서 B(epoch 6)로 넘어간 뒤 저장 워커가 lease 테이블의 epoch와 비교하는 조건부 `INSERT` 로 씁니다(SQL은 [results/Q5.md](results/Q5.md)의 `LEASE_EQ`). 로그에는 A의 레코드가 있지만 message 테이블에는 B의 레코드만 있는지, A가 lease를 잃기 전에 쓴 레코드는 정상으로 들어가는지 확인합니다.

**Q6.** `read_committed` 전달 지연, 트랜잭션 처리량, 조건부 `INSERT` 비용, 담당 교체 공백을 잽니다. 측정 조건은 [results/Q6.md](results/Q6.md)에 있습니다.

## 결과를 쓰는 곳

- Q1, Q2: 브로커가 옛 담당자를 막는 조건과 거부 시점
- Q3, Q4: 소비자 격리 수준과 담당자 기동 순서 규칙
- Q5, Q6: 막는 위치를 브로커와 저장소 중 어디로 둘지

## 진행 순서

1. Gradle Wrapper, Spring Boot, Testcontainers 구성과 colima 연결 확인
2. Q1, Q2, Q3
3. Q4
4. Q5
5. Q6 측정, 결과 문서와 README 작성
