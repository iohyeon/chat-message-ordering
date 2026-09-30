package lab.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lab.shard.ShardProducer;
import lab.zombie.ZombieGate;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;

/**
 * Q2, Q3가 함께 쓰는 fencing 시나리오.
 *
 * <pre>
 * A: initTransactions, (기록) begin, send(seq 100), commit
 * A: 멈추기 전 상태 준비 (prior)
 *      NONE  begin 만 하고 아무것도 보내지 않음
 *      SENT  begin, send(seq 101), flush
 *      IDLE  트랜잭션을 열지 않음 (READY)
 * A: 멈춤 (ZombieGate)
 * B: 같은 transactional.id 로 initTransactions   (successor=false 이면 B 없이 transaction.timeout.ms 만료를 기다림)
 * A: 깨어나 ops 를 차례로 실행하고 각 결과를 기록
 * B: begin, send(seq 101), commit
 * </pre>
 */
final class FencingScenario {

    enum Prior { NONE, SENT, IDLE }

    static final List<String> TXN_APIS = List.of("INIT_PRODUCER_ID", "PRODUCE", "ADD_PARTITIONS_TO_TXN", "END_TXN",
            "WRITE_TXN_MARKERS");

    final String id;
    final String txnId;
    final String topic;
    final Prior prior;
    final boolean successor;
    final List<String> ops;
    final Map<String, Object> aOverrides;

    /** 관찰 결과. ops 의 i번째 결과는 "op#i" 키로 들어간다. send는 Future와 콜백 두 개가 들어간다. */
    final Map<String, Report.Outcome> outcomes = new LinkedHashMap<>();
    final Map<String, String> facts = new LinkedHashMap<>();
    String dump;
    List<LabBroker.RequestLine> requests = List.of();

    FencingScenario(String id, Prior prior, boolean successor, List<String> ops, Map<String, Object> aOverrides) {
        this.id = id;
        this.txnId = "q-" + id;
        this.topic = "q-" + id + "-log";
        this.prior = prior;
        this.successor = successor;
        this.ops = ops;
        this.aOverrides = aOverrides;
    }

    FencingScenario(String id, Prior prior, List<String> ops) {
        this(id, prior, true, ops, Map.of());
    }

    void run(LabBroker broker, ClientLogCapture clientLogs, Report r) throws Exception {
        broker.createTopic(topic);
        clientLogs.clear();
        r.section("시나리오 " + id + " (prior=" + prior + ", 후임 B=" + successor + ", ops=" + ops + ")");

        Map<String, Object> aConfig = new HashMap<>(aOverrides);
        aConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "A@" + id);
        ShardProducer a = ShardProducer.transactional(broker.bootstrap(), "A", topic, txnId, aConfig);
        ZombieGate gate = new ZombieGate(Duration.ofSeconds(120));
        List<ShardProducer.Sent> aPending = new ArrayList<>();

        CompletableFuture<Void> aThread = CompletableFuture.runAsync(() -> {
            record("A initTransactions", r.call("A initTransactions", () -> a.producer().initTransactions()));
            fact(r, "A 초기 producerIdAndEpoch", a.producerIdAndEpoch());
            r.call("A begin (seq 100 기록용)", () -> a.producer().beginTransaction());
            r.sendResult("A send(100)", a.send(100), 30);
            r.call("A commit (seq 100)", () -> a.producer().commitTransaction());
            fact(r, "A seq 100 커밋 뒤 producerIdAndEpoch", a.producerIdAndEpoch());
            if (prior != Prior.IDLE) {
                r.call("A begin", () -> a.producer().beginTransaction());
            }
            if (prior == Prior.SENT) {
                r.sendResult("A send(101) 멈추기 전", a.send(101), 30);
                a.producer().flush();
            }
            fact(r, "A 멈춤 직전 producerIdAndEpoch", a.producerIdAndEpoch());
            fact(r, "A 멈춤 직전 클라이언트 상태", a.transactionState());
            gate.pauseHere();

            r.line("  -- A 깨어남 --");
            long nextSeq = prior == Prior.SENT ? 102 : 101;
            for (int i = 0; i < ops.size(); i++) {
                String op = ops.get(i);
                String key = "op#" + i + " " + op;
                switch (op) {
                    case "send" -> {
                        ShardProducer.Sent sent;
                        try {
                            sent = a.send(nextSeq);
                        } catch (Exception e) {
                            record(key + " (send가 직접 던짐)", r.call(key + " send()가 직접 던짐", () -> {
                                throw e;
                            }));
                            break;
                        }
                        aPending.add(sent);
                        Report.SendOutcome so = r.sendResult("A " + op + "(" + nextSeq + ")", sent, 15);
                        outcomes.put(key + " future", so.future());
                        outcomes.put(key + " callback", so.callback());
                        facts.put(key + " callbackThread", String.valueOf(so.callbackThread()));
                        nextSeq++;
                    }
                    case "commit" -> record(key, r.call("A commitTransaction", () -> a.producer().commitTransaction()));
                    case "abort" -> record(key, r.call("A abortTransaction", () -> a.producer().abortTransaction()));
                    case "begin" -> record(key, r.call("A beginTransaction", () -> a.producer().beginTransaction()));
                    default -> throw new IllegalArgumentException(op);
                }
                fact(r, key + " 뒤 A 클라이언트 상태", a.transactionState());
            }
        });

        gate.awaitPaused();
        fact(r, "fencing 전 조정자 상태", broker.describeTxnLine(txnId));
        fact(r, "fencing 전 파티션 producer 상태", broker.describeProducersLine(topic));
        fact(r, "fencing 전 HW", String.valueOf(broker.endOffset(topic, IsolationLevel.READ_UNCOMMITTED)));
        fact(r, "fencing 전 LSO", String.valueOf(broker.endOffset(topic, IsolationLevel.READ_COMMITTED)));

        ShardProducer b = null;
        if (successor) {
            b = ShardProducer.transactional(broker.bootstrap(), "B", topic, txnId,
                    Map.of(ProducerConfig.CLIENT_ID_CONFIG, "B@" + id));
            ShardProducer bb = b;
            record("B initTransactions", r.call("B initTransactions", () -> bb.producer().initTransactions()));
            fact(r, "B producerIdAndEpoch", b.producerIdAndEpoch());
        } else {
            // 후임 없이 A의 transaction.timeout.ms 가 지나 조정자가 스스로 중단하기를 기다린다.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            String line = broker.describeTxnLine(txnId);
            while (!line.contains("CompleteAbort") && System.nanoTime() < deadline) {
                Thread.sleep(500);
                line = broker.describeTxnLine(txnId);
            }
            fact(r, "시간 초과 대기 뒤 조정자 상태", line);
        }
        fact(r, "fencing 뒤 조정자 상태", broker.describeTxnLine(txnId));
        fact(r, "fencing 뒤 파티션 producer 상태", broker.describeProducersLine(topic));
        fact(r, "fencing 뒤 HW", String.valueOf(broker.endOffset(topic, IsolationLevel.READ_UNCOMMITTED)));
        fact(r, "fencing 뒤 LSO", String.valueOf(broker.endOffset(topic, IsolationLevel.READ_COMMITTED)));

        gate.resume();
        aThread.get(120, TimeUnit.SECONDS);

        if (b != null) {
            r.line("  -- B 수락 --");
            ShardProducer bb = b;
            r.call("B begin", () -> bb.producer().beginTransaction());
            Report.SendOutcome bs = r.sendResult("B send(101)", b.send(101), 30);
            outcomes.put("B send future", bs.future());
            record("B commit", r.call("B commit", () -> bb.producer().commitTransaction()));
            fact(r, "B 커밋 뒤 producerIdAndEpoch", b.producerIdAndEpoch());
        }

        r.line("  -- A close --");
        r.call("A close", a::close);
        for (ShardProducer.Sent s : aPending) {
            r.line("  close 뒤 A send(" + s.seq() + ") Future.isDone=" + s.future().isDone()
                    + " callback.isDone=" + s.callback().isDone());
        }
        if (b != null) {
            b.close();
        }

        fact(r, "마지막 조정자 상태", broker.describeTxnLine(txnId));
        fact(r, "마지막 파티션 producer 상태", broker.describeProducersLine(topic));

        dump = broker.dumpLog(topic);
        r.line("  kafka-dump-log 요약:");
        LabBroker.summarizeDump(dump).forEach(l -> r.line("    " + l));

        r.line("  브로커 요청 로그 (" + TXN_APIS + ", " + txnId + " 또는 " + topic + " 포함):");
        requests = broker.requestLines(TXN_APIS, "\"" + txnId + "\"", "\"" + topic + "\"");
        requests.forEach(l -> r.line("    " + l.render()));

        r.line("  브로커 로그 (" + txnId + " 또는 " + topic + " 포함):");
        broker.brokerLines(l -> l.contains(txnId) || l.contains(topic + "-0")).forEach(l -> r.line("    " + l));

        r.line("  A 클라이언트 로그 (상태 전이, 에러):");
        clientLogs.lines(l -> l.contains("A@" + id) && (l.contains("Transition") || l.contains("Transiting")
                        || l.contains("ProducerId set") || l.contains("rror") || l.contains("fenced")
                        || l.contains("Aborting") || l.contains("EndTxn") || l.contains("abort")))
                .forEach(l -> r.line("    " + l));
        r.line("  B 클라이언트 로그 (producer ID 할당, 재시도):");
        clientLogs.lines(l -> l.contains("B@" + id) && (l.contains("ProducerId set") || l.contains("CONCURRENT")
                        || l.contains("retry") || l.contains("Retry")))
                .forEach(l -> r.line("    " + l));
    }

    private void record(String key, Report.Outcome o) {
        outcomes.put(key, o);
    }

    private void fact(Report r, String key, String value) {
        facts.put(key, value);
        r.line("  [" + key + "] " + value);
    }

    Report.Outcome op(int i) {
        String prefix = "op#" + i + " ";
        return outcomes.entrySet().stream().filter(e -> e.getKey().startsWith(prefix)).findFirst().orElseThrow().getValue();
    }

    /** A 클라이언트가 보낸 요청 중 api 에 해당하는 것들의 에러 코드 목록(요청 순서). */
    List<List<Integer>> errorsOf(String api, String clientId) {
        return requests.stream().filter(l -> l.api().equals(api) && l.clientId().equals(clientId))
                .map(LabBroker.RequestLine::errors).toList();
    }

    Report.Outcome opCallback(int i) {
        String prefix = "op#" + i + " ";
        return outcomes.entrySet().stream().filter(e -> e.getKey().startsWith(prefix) && e.getKey().endsWith("callback"))
                .findFirst().orElseThrow().getValue();
    }
}
