# chat-message-ordering

채팅 메시지의 순번(`conversation_seq`)을 매기는 writer가 교체될 때, 이전 세대(epoch) writer, 즉 zombie writer의 쓰기를 **Kafka 브로커에서 막는 방식**과 **저장소에서 막는 방식**을 직접 재현해 비교한 저장소입니다. zombie writer는 lease를 잃은 것을 모른 채 깨어나 계속 쓰려는 writer로, KIP-98이 "zombie instance"라고 부르는 것과 같습니다. 이 문서에서는 이후 이전 세대 writer, 교체 뒤의 writer를 현재 writer라고 씁니다. Kafka 4.3.1 단일 브로커와 PostgreSQL 16을 Testcontainers로 띄우고 질문 7개를 JUnit 테스트 29개와 측정 코드로 확인했습니다. 브로커는 이전 세대 writer의 기록 요청을 **보내는 시점에 `InvalidProducerEpochException`** 으로 거부했고, 저장소의 조건부 `INSERT` 는 lease 교체와 20,000번 경쟁시켰을 때 **139번 같은 seq를 잃고 그때마다 현재 writer의 레코드가 버려졌습니다**(비교하는 행을 잠그는 `FOR SHARE` 와 `LOG_ORDER` 는 0번).

확인한 질문은 다음과 같습니다.

- 멱등 producer만 쓰면 이전 세대 writer의 쓰기가 막히는가
- 트랜잭션 producer에서 이전 세대 writer는 어느 호출에서 어떤 예외로 거부되는가
- 중단(abort)된 트랜잭션의 레코드는 어느 격리 수준의 소비자에게 보이는가
- 현재 writer가 로그 replay를 `initTransactions()` 보다 먼저 하면 seq가 겹치는가
- 저장소의 조건부 쓰기(fencing token)는 이전 세대 writer의 레코드만 정확히 걸러 내는가
- 두 방식의 전달 지연, 처리량, 저장 비용, 교체 공백은 얼마인가
- 저장소 fencing의 세 조건(로그 순서 기준, 비교하는 행의 잠금, 저장 뒤 응답)을 함께 쓰면 경쟁이 없어지는가, 응답은 얼마나 늦어지는가

## Highlights

| 실험 | 확인한 것 | 방법 | 결과 |
| --- | --- | --- | --- |
| [Q1](results/Q1.md) 멱등 producer | 이전 세대 writer의 쓰기가 막히는가 | producer 두 개가 같은 파티션에 같은 seq를 쓰고, `kafka-dump-log` 로 배치 헤더를 확인 | 막히지 않음. 두 writer가 producerId 0과 1을 따로 받았고, 이전 세대 writer가 깨어나 보낸 seq 103까지 레코드 5개가 모두 기록됨 |
| [Q2](results/Q2.md) 트랜잭션 fencing | 거부되는 호출과 예외 클래스 | 같은 `transactional.id` 로 현재 writer가 `initTransactions()` 한 뒤 이전 세대 writer의 호출을 시나리오 8개로 나눠 기록하고, 브로커 요청 로그와 조정자 로그로 대조 | 기록 요청은 보내는 시점에 `InvalidProducerEpochException`(에러 코드 47), 커밋과 중단은 `ProducerFencedException`(90). 보낸 것이 없는 트랜잭션의 커밋과 `beginTransaction()` 은 성공. 후임 없이 시간 초과로 중단된 경우(약 9.3초)에는 이전 세대 writer가 중단 뒤 다시 커밋까지 함 |
| [Q3](results/Q3.md) abort 레코드 가시성 | 중단된 레코드가 보이는 격리 수준 | Q2의 로그를 두 격리 수준의 소비자로 처음부터 읽고 FETCH 응답을 비교 | `read_uncommitted` 는 seq 101을 두 번 받음. 브로커는 두 소비자에게 같은 555바이트를 보내고, 걸러 내는 곳은 `read_committed` 소비자 클라이언트 |
| [Q4](results/Q4.md) 기동 순서 | replay를 `initTransactions()` 보다 먼저 하면 seq가 겹치는가 | 이전 세대 writer를 커밋 직전에 멈추고, 현재 writer의 replay와 `initTransactions()` 사이에서 깨움 | 겹침. seq 101, 102가 `read_committed` 에 두 번씩 보임. `initTransactions()` 를 먼저 하면 한 번씩. 열린 트랜잭션이 있을 때 `initTransactions()` 242ms, 없을 때 121ms |
| [Q5](results/Q5.md) 저장소 fencing | 이전 세대 레코드가 저장에서 걸러지는가 | 멱등 producer만 쓰고, 저장 워커가 lease 테이블의 epoch와 비교하는 조건부 `INSERT` 로 씀. JDBC 연결 두 개로 경쟁 순서를 직접 만듦 | 걸러짐. 다만 lease 테이블과 비교하면 교체 전에 정당하게 쓴 레코드도 거부됨. lease 교체와 경쟁시킨 20,000번 중 **139번**(`fsync=on`) 같은 seq를 잃고, 139번 모두 현재 writer의 레코드가 `ON CONFLICT DO NOTHING` 으로 버려짐. `FOR SHARE` 는 **0번**. 두 writer의 동시 lease 획득 300번에서 둘 다 성공 0번. 이전 세대 writer는 저장에서 거부된 3건을 모두 성공으로 응답함 |
| [Q6](results/Q6.md) 비용 | 두 방식의 지연, 처리량, 저장 비용, 교체 공백 | 커밋 주기, 트랜잭션당 레코드 수, 동시 연결 수를 바꿔 가며 3회 반복 측정 | 커밋 주기 100ms에서 `read_committed` p50 **52.6ms**, p99 **102.6ms**(멱등 기준선 p50 0.5ms, p99 12.2ms). 교체 뒤 수락 재개까지 브로커 방식 p50 **144.5ms**, 저장소 방식 **2.5ms**. 조건부 `INSERT` 와 `FOR SHARE` 는 단순 `INSERT` 와 반복 간 흔들림 안에서 구별되지 않음 |
| [Q7](results/Q7.md) 저장소 fencing의 세 조건 | 로그 순서 기준에도 경쟁이 없는가, 저장 뒤 응답은 얼마나 늦는가 | `LOG_ORDER` 와 잠금을 뺀 대조군을 20,000번씩 경쟁시키고, 스냅숏 epoch로 거부를 나누고, 초당 1,000건에서 두 방식의 수락 지연을 같은 실행 안에서 번갈아 잼 | `LOG_ORDER` 경쟁 **0번**, 잠금을 뺀 대조군은 경쟁 구간을 넓히면 1,994번 중 **1,867번**. 저장 완료 p50 **0.83ms**, p99 **146.9ms**(브로커 fencing 커밋 주기 100ms: p50 53.0ms, p99 141.5ms). 한 대화 집중 부하의 처리량 하락은 fence 행 배타 잠금 때문(배타 잠금만 19%, 공유 잠금 86%) |

Q1부터 Q5의 출력 발췌는 [results/raw/](results/raw/) 의 테스트 출력에서, Q5 반복 측정과 Q6, Q7의 표는 [results/data/](results/data/) 의 CSV에서 옮겼습니다.

## Findings

두 방식 모두 이전 세대 writer의 레코드를 걸러 냈지만, 각각 아래 조건이 모두 있어야 성립했습니다. 어느 쪽을 쓸지는 이 저장소에서 정하지 않고, 조건과 비용만 적습니다.

### 브로커 fencing이 성립하는 조건

1. **트랜잭션 producer.** 멱등 producer는 프로세스마다 새 producerId를 받으므로 브로커가 두 writer를 연결할 근거가 없습니다(Q1).
2. **모든 소비자가 `read_committed`.** 중단된 레코드는 로그에 남고, `read_uncommitted`(기본값) 소비자는 그것을 받습니다(Q3).
3. **기동 순서가 lease 획득, `initTransactions()`, replay.** replay를 먼저 하면 LSO에서 멈춘 채 마지막 seq를 잘못 읽고, 그 사이 이전 세대 writer의 커밋이 성공합니다(Q4). lease가 끝난 것만으로는 브로커가 막지 않고, 현재 writer의 `initTransactions()` 가 불려야 막힙니다(Q2 e-timeout).

비용은 전달 지연이 커밋 주기만큼 늘어나는 것(p99가 커밋 주기와 거의 같음)과, 교체 공백의 대부분을 차지하는 `initTransactions()` 의 재시도 대기(`retry.backoff.ms`)입니다. 트랜잭션당 1건 설정에서 드물게 `InvalidTxnStateException` 으로 producer가 fatal 상태가 됐고, 원인은 찾지 못했습니다(Q6).

### 저장소 fencing이 성립하는 조건

1. **비교 기준이 로그 순서.** lease 테이블의 현재 epoch와 비교하면, 저장 워커가 밀려 있을 때 교체 전에 정당하게 쓴 레코드까지 거부됩니다. 로그에 교체 표시 레코드를 쓰고 저장소가 본 가장 큰 epoch와 비교하면(`LOG_ORDER`) 이 유실이 없어졌습니다(Q5 b).
2. **비교하는 행의 잠금.** READ COMMITTED의 서브쿼리는 읽은 행을 잠그지 않으므로, 워커의 삽입이 커밋되기 전에 현재 writer가 교체와 마지막 seq 읽기를 끝내면 같은 seq를 잃습니다. lease 테이블과 비교하면 lease 행에 `FOR SHARE` 를 붙이거나 SERIALIZABLE로 막혔습니다(Q5 c). `LOG_ORDER` 는 비교에 쓰는 fence 행을 문장이 갱신하며 잠그므로 따로 잠금을 더하지 않아도 20,000번 경쟁에서 0번이었고, 잠금을 뺀 대조군은 경쟁 구간을 넓히자 1,994번 중 1,867번 어긋났습니다(Q7 (1)).
3. **사용자 응답을 저장 결과 뒤로.** 브로커는 이전 세대 writer의 전송을 모두 받아 주므로, 브로커 ack를 보고 응답하면 저장에서 거부된 메시지를 성공으로 알립니다(Q5 d).

비용은 다음과 같습니다.

- `FOR SHARE` 의 문장 비용은 측정에서 보이지 않았습니다(Q6). `FOR SHARE` 가 lease 교체와 겹친 레코드를 더 거부하는 범위는 교체 `UPDATE` 가 걸린 동안뿐이었고, 교체 전에 쓴 레코드의 대량 거부는 lease 테이블 비교와 저장 워커 밀림에서 나왔습니다(`LOG_ORDER` 계열은 0건, Q7 (2)).
- `LOG_ORDER` 는 한 대화에 쓰기가 몰리면 처리량이 단순 `INSERT` 의 16~18%(`fsync=on`, 연결 32)로 떨어졌습니다. 레코드마다 fence 행을 갱신하며 쥐는 배타 잠금 때문이고, fence 행을 `FOR SHARE` 로 읽는 변형은 86%였고 경쟁도 0번이었습니다(Q6, Q7 (4)).
- 3번 조건의 비용은 저장 워커의 처리 지연입니다. 워커가 따라가는 동안 p50은 브로커 ack보다 약 0.4ms 늘지만, 워커 하나가 순서대로 저장하므로 문장 하나의 멈춤 뒤에 레코드가 줄을 서서 p99는 커밋 주기 100ms의 브로커 fencing과 비슷했습니다. 워커 처리량(이 환경의 per-record 워커는 `fsync=off` 에서 초당 약 2,400~2,900건)을 넘으면 지연이 수 초로 늘었습니다(Q7 (3)).

## Reproducing the experiments

필요한 것은 JDK 21과 Docker 호환 런타임입니다. colima에서는 다음 환경 변수를 씁니다.

```bash
export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```

Q1부터 Q5의 테스트(29개)는 다음 명령 하나로 돕니다. Q1부터 Q4의 결과를 만든 실행은 1분 56초, `Q5LeaseRaceTest.c5` 에 `LOG_ORDER` 계열을 더한 지금 코드의 실행은 3분 15초가 걸렸습니다.

```bash
./gradlew test
```

테스트는 관찰 내용을 `build/lab-output/*.txt` 에 남깁니다. [results/raw/](results/raw/) 의 Q1부터 Q4 파일은 그 한 번의 실행을, Q5 파일은 `./gradlew test --tests 'lab.store.*'` 로 따로 돌린 실행을 그대로 복사한 것입니다.

Q5의 반복 측정과 Q6, Q7의 측정은 일반 테스트에서 빠져 있고, 클래스마다 따로 돌립니다.

```bash
./gradlew benchmark --tests 'lab.bench.Q5RaceSoakBenchmark'
./gradlew benchmark --tests 'lab.bench.Q6DeliveryLatencyBenchmark'
./gradlew benchmark --tests 'lab.bench.Q6TxThroughputBenchmark'
./gradlew benchmark --tests 'lab.bench.Q6StoreInsertBenchmark'
./gradlew benchmark --tests 'lab.bench.Q6TakeoverGapBenchmark'
./gradlew benchmark --tests 'lab.bench.Q7ForShareRejectDiagnostics'
./gradlew benchmark --tests 'lab.bench.Q7PreTakeoverRejectBenchmark'
./gradlew benchmark --tests 'lab.bench.Q7AcceptLatencyBenchmark'
./gradlew benchmark --tests 'lab.bench.Q7FenceWriteBenchmark'
```

측정 구간 설정으로 계산한 실행 시간의 하한은 전달 지연 약 7분, 트랜잭션 처리량 약 6.5분, 저장 비용 약 24분(`fsync=off`, `fsync=on` 합)입니다. Q5 반복 측정은 기록된 실행에서 열 설정 합계 약 10분이었습니다. 측정 코드는 결과를 [results/data/](results/data/) 의 CSV에 씁니다. 클래스에 따라 기존 파일을 지우고 새로 쓰거나(Q6 대부분, `Q7FenceWriteBenchmark`, 원 표본 `*.csv.gz`) 기존 행 뒤에 이어 붙입니다(`Q5RaceSoakBenchmark`, `Q7AcceptLatencyBenchmark` 의 요약 등). 진단용 클래스(`Q6TxFailureDiagnostics` 등)와 그 명령은 [Q6](results/Q6.md) 에 있습니다.

한계는 다음과 같습니다.

- 브로커 한 대, 파티션 하나, 복제 없음. `acks=all` 이어도 ISR이 브로커 하나뿐입니다.
- 로컬 노트북의 colima VM(CPU 4개, 메모리 8GB)에서 잰 값입니다. 측정 JVM은 VM 밖에서 돌고 컨테이너와 포트 포워딩으로 통신합니다.
- 측정 중 같은 VM에 다른 프로젝트의 컨테이너(Kafka 브로커 하나, ClickHouse 하나)가 떠 있었고, 그 브로커가 가끔 CPU를 썼습니다. 부하는 [Q6](results/Q6.md) 의 각 절과 [Q7](results/Q7.md) 의 측정 조건에 기록했습니다. Q7 측정 중에는 호스트의 다른 프로그램 부하도 컸고, 그 뒤의 측정은 호스트 load average를 함께 남겼습니다.
- 그래서 Q6, Q7의 수치는 절대값이 아니라 같은 환경에서의 상대 비교로만 씁니다.
- 모든 결과는 `transaction.version=2`(KIP-890) 기준입니다.
- 저장소는 PostgreSQL만 쟀습니다. wide-column 저장소의 조건부 쓰기 비용은 재지 않았습니다.

## Stack

| 항목 | 버전 | 쓰는 곳 |
| --- | --- | --- |
| Java | 21 | |
| Spring Boot | 4.1.1 | 의존성 관리와 JDBC 구성 |
| `kafka-clients`, 브로커 이미지 `apache/kafka` | 4.3.1 | Spring Kafka 없이 클라이언트를 직접 써서 `initTransactions()` 호출 시점과 producer 재생성을 드러냅니다 |
| PostgreSQL | 16 | lease 테이블과 조건부 `INSERT ... SELECT ... WHERE ... ON CONFLICT` |
| Testcontainers | 2.0.5 | 시나리오마다 브로커와 DB를 새로 띄웁니다 |
| Gradle | 9.8.0 | |

## References

- [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)
- [KIP-588: Allow producers to recover gracefully from transaction timeouts](https://cwiki.apache.org/confluence/display/KAFKA/KIP-588%3A+Allow+producers+to+recover+gracefully+from+transaction+timeouts)
- [KIP-890: Transactions Server-Side Defense](https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense)
- [Apache Kafka Design](https://kafka.apache.org/43/design/design/)
- Martin Kleppmann, [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) (2016)

각 결과 문서의 "근거" 절에 Kafka 4.3.1 소스의 해당 줄 링크가 있습니다.

## Notes

채팅 설계는 『가상 면접 사례로 배우는 대규모 시스템 설계 기초』(알렉스 쉬, 인사이트)를 읽고 확장한 설계에서 출발했습니다. 책의 내용을 요약하거나 옮기지 않았고, 실험 설계와 코드는 Kafka 공식 문서, KIP, 소스 코드를 바탕으로 직접 작성했습니다. 책의 저작권은 저자와 출판사에 있습니다.

실험 설계와 진행 순서는 [PLAN.md](PLAN.md)에 있습니다. 실험 결과와 설계 과정은 [기술 블로그](https://develop-tracking.tistory.com/)에 씁니다. 틀린 부분은 이슈로 알려 주세요.

## License

[MIT](LICENSE)
