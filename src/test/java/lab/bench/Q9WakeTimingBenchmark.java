package lab.bench;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lab.Containers;
import lab.store.ChatRecord;
import lab.store.CommittedReplay;
import lab.store.Q8Support;
import lab.store.Q9Support;
import lab.store.StoreFixture;
import lab.store.TxSeqWriter;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Q9 (2). 후임 없는 시간 초과 중단과 담당자가 깨어나는 시점의 관계를 반복해서 잰다.
 *
 * <p>한 번: 담당자 A가 seq 100을 커밋하고, 101, 102를 보낸 뒤(flush, 커밋 전) {@code pause} 동안 멈춘다. {@code pause} 는
 * [0, {@link #PAUSE_MAX_MS}) 에서 고르게 뽑는다. 깨어나 곧바로 commit 하거나(COMMIT), 한 건 더 보내고 commit 한다(SEND).
 * 실패하면 처리 방식 (a) 같은 producer로 abort 후 순번을 되감아 계속, 또는 (b) 새 producer로 재기동(init, replay)을 번갈아
 * 고른다. (a)의 abort가 실패하면 (b)로 넘어간다. 마지막에 2건을 더 수락하고 read_committed 로그로 판정한다.
 * 후임 담당자는 없고 lease는 계속 A의 것이라고 둔다.
 */
@Testcontainers
@Tag("benchmark")
class Q9WakeTimingBenchmark {

    static final int TXN_TIMEOUT_MS = 1500;
    static final String CLEANUP_INTERVAL_MS = "500";
    static final int TRIALS = 240;
    static final long PAUSE_MAX_MS = 3000;
    static final String RAW = "q9_wake_timing.csv";
    static final String SUMMARY = "q9_wake_timing_summary.csv";

    @Container
    static final KafkaContainer KAFKA = Containers.kafka()
            .withEnv("KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS", CLEANUP_INTERVAL_MS)
            // A의 EndTxn 응답 에러 코드를 남기려고 요청 로그를 켠다.
            .withEnv("KAFKA_LOG4J_LOGGERS", "kafka.request.logger=DEBUG");

    enum Wake { COMMIT, SEND }

    enum Policy { A_ABORT_REWIND, B_RESTART }

    record Trial(int trial, Wake wake, Policy policy, long pauseMs, long wakeMs, long describeMs,
                 String stateBeforeWake, long txnStartVmMs,
                 String sendError, String commitError, long commitMs, String handling, String abortError,
                 long restartInitMs, long replayLastSeq, Q9Support.Verdict verdict, int acked, int failed,
                 String txId) {
    }

    @Test
    void wakeTiming() throws Exception {
        Bench.env("Q9-2 wake timing");
        Bench.deleteIfExists(RAW);
        Bench.deleteIfExists(SUMMARY);
        String bootstrap = KAFKA.getBootstrapServers();
        Random rnd = new Random(9_2026L);
        List<Trial> trials = new ArrayList<>();
        long t00 = System.nanoTime();
        try (Admin admin = Q8Support.admin(bootstrap)) {
            for (int i = 0; i < TRIALS; i++) {
                Wake wake = Wake.values()[i % 2];
                Policy policy = Policy.values()[(i / 2) % 2];
                long pause = (long) (rnd.nextDouble() * PAUSE_MAX_MS);
                trials.add(trial(i, wake, policy, pause, bootstrap, admin));
                if ((i + 1) % 20 == 0) {
                    System.out.printf(Locale.ROOT, "PROGRESS trials=%d elapsed=%.1fs%n", i + 1,
                            (System.nanoTime() - t00) / 1e9);
                }
            }
        }
        String logs = KAFKA.getLogs();
        Map<String, Long> rollbacks = rollbackTimes(logs);
        Map<String, List<String>> endTxn = endTxnErrors(logs);
        String header = "trial,wake,policy,pause_ms,wake_ms,describe_ms,state_before_wake,send_error,commit_error,commit_ms,handling,"
                + "abort_error,restart_init_ms,replay_last_seq,txn_start_vm_ms,rollback_vm_ms,rollback_after_start_ms,"
                + "acked,failed,broker_acked_aborted,dup_seqs,gaps,acked_lost,failed_stored,a_end_txn_errors";
        for (Trial t : trials) {
            Long rb = rollbacks.get(t.txId());
            Bench.append(RAW, header, String.format(Locale.ROOT,
                    "%d,%s,%s,%d,%d,%d,%s,%s,%s,%d,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s",
                    t.trial(), t.wake(), t.policy(), t.pauseMs(), t.wakeMs(), t.describeMs(), t.stateBeforeWake(), t.sendError(), t.commitError(),
                    t.commitMs(), t.handling(), t.abortError(), t.restartInitMs(), t.replayLastSeq(), t.txnStartVmMs(),
                    rb == null ? -1 : rb, rb == null ? -1 : rb - t.txnStartVmMs(), t.acked(), t.failed(),
                    t.verdict().brokerAckedAborted().size(), t.verdict().duplicateSeqs().size(), t.verdict().gaps().size(),
                    t.verdict().ackedLost().size(), t.verdict().failedStored().size(),
                    String.join(" ", endTxn.getOrDefault("A@" + t.txId(), List.of()))));
        }
        // 요약: 깨어나는 방식, 깨어나기 직전 조정자 상태, 결과별 횟수와 pause 범위
        Map<String, long[]> groups = new TreeMap<>();
        for (Trial t : trials) {
            String outcome = t.commitError().equals("-") ? "commit_ok" : t.commitError();
            String key = t.wake() + "," + t.stateBeforeWake() + "," + (t.sendError().equals("-") ? "-" : t.sendError())
                    + "," + outcome;
            long[] g = groups.computeIfAbsent(key, k -> new long[] {0, Long.MAX_VALUE, Long.MIN_VALUE, 0, 0, 0, 0});
            g[0]++;
            g[1] = Math.min(g[1], t.pauseMs());
            g[2] = Math.max(g[2], t.pauseMs());
            g[3] += t.verdict().duplicateSeqs().size();
            g[4] += t.verdict().gaps().size();
            g[5] += t.verdict().ackedLost().size();
            g[6] += t.verdict().failedStored().size();
        }
        String sh = "wake,state_before_wake,send_error,commit_outcome,n,pause_min_ms,pause_max_ms,dup_seqs,gaps,"
                + "acked_lost,failed_stored";
        groups.forEach((k, g) -> {
            String line = String.format(Locale.ROOT, "%s,%d,%d,%d,%d,%d,%d,%d", k, g[0], g[1], g[2], g[3], g[4], g[5], g[6]);
            Bench.append(SUMMARY, sh, line);
            System.out.println("SUMMARY " + line);
        });
    }

    Trial trial(int i, Wake wake, Policy policy, long pauseMs, String bootstrap, Admin admin) throws Exception {
        String id = i + "-" + System.nanoTime();
        String topic = "q9-wake-" + id;
        String conv = "conv-wake-" + id;
        String txId = "tx-" + conv;
        StoreFixture.createTopic(bootstrap, topic);
        Map<String, Object> cfg = Map.of(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, TXN_TIMEOUT_MS);
        List<TxSeqWriter> writers = new ArrayList<>();
        TxSeqWriter a = new TxSeqWriter("A", bootstrap, topic, conv, txId, 5, cfg);
        writers.add(a);
        String sendError = "-";
        String commitError = "-";
        String abortError = "-";
        String handling = "none";
        long restartInitMs = -1;
        long replayLast = -1;
        long commitMs;
        String stateBefore;
        long txnStart;
        long describeMs;
        long wakeMs;
        try {
            a.initTransactions();
            a.startAt(100);
            a.begin();
            a.send(1);
            a.commit();
            a.begin();
            a.send(2);
            long tFlush = System.nanoTime();
            TransactionDescription d0 = admin.describeTransactions(List.of(txId)).description(txId).get(10, TimeUnit.SECONDS);
            txnStart = d0.transactionStartTimeMs().orElse(-1);
            long wakeAt = tFlush + TimeUnit.MILLISECONDS.toNanos(pauseMs);
            long now = System.nanoTime();
            if (wakeAt > now) {
                TimeUnit.NANOSECONDS.sleep(wakeAt - now);
            }
            long dsc0 = System.nanoTime();
            stateBefore = admin.describeTransactions(List.of(txId)).description(txId).get(10, TimeUnit.SECONDS)
                    .state().toString();
            long dsc1 = System.nanoTime();
            describeMs = TimeUnit.NANOSECONDS.toMillis(dsc1 - dsc0);
            wakeMs = TimeUnit.NANOSECONDS.toMillis(dsc1 - tFlush);
            // 깨어남
            boolean failed = false;
            long c0 = System.nanoTime();
            if (wake == Wake.SEND) {
                try {
                    a.send(1);
                } catch (Exception e) {
                    sendError = e.getClass().getSimpleName();
                }
            }
            try {
                a.commit();
            } catch (RuntimeException e) {
                commitError = e.getClass().getSimpleName();
                failed = true;
            }
            commitMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - c0);
            TxSeqWriter current = a;
            if (failed) {
                boolean restart = true;
                if (policy == Policy.A_ABORT_REWIND) {
                    long first = 101;
                    try {
                        a.abort();
                        a.rewindTo(first);
                        handling = "abort_rewind";
                        restart = false;
                    } catch (RuntimeException e) {
                        abortError = e.getClass().getSimpleName();
                    }
                }
                if (restart) {
                    handling = policy == Policy.A_ABORT_REWIND ? "abort_failed_restart" : "restart";
                    a.close();
                    TxSeqWriter a2 = new TxSeqWriter("A2", bootstrap, topic, conv, txId, 5, cfg);
                    writers.add(a2);
                    long r0 = System.nanoTime();
                    a2.initTransactions();
                    restartInitMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - r0);
                    CommittedReplay.Result rp = CommittedReplay.replay(bootstrap, topic, "A2-replay-" + id,
                            Duration.ofSeconds(20));
                    replayLast = rp.lastSeq();
                    a2.startAt(rp.lastSeq() + 1);
                    current = a2;
                }
            }
            current.begin();
            current.send(2);
            current.commit();
        } finally {
            for (TxSeqWriter w : writers) {
                w.close();
            }
        }
        Q8Support.awaitStable(admin, topic);
        List<ChatRecord> log = CommittedReplay.readAll(bootstrap, topic, Duration.ofSeconds(20));
        Q9Support.Verdict v = Q9Support.judge(log, 100, writers);
        int acked = writers.stream().mapToInt(w -> w.ackedToUser().size()).sum();
        int failedN = writers.stream().mapToInt(w -> w.failedToUser().size()).sum();
        return new Trial(i, wake, policy, pauseMs, wakeMs, describeMs, stateBefore, txnStart, sendError, commitError, commitMs, handling,
                abortError, restartInitMs, replayLast, v, acked, failedN, txId);
    }

    static final Pattern CLIENT_ID = Pattern.compile("\"clientId\":\"([^\"]+)\"");
    static final Pattern RESPONSE_ERROR = Pattern.compile("\"response\":\\{\"throttleTimeMs\":\\d+,\"errorCode\":(-?\\d+)");

    /** 요청 로그의 END_TXN 응답 에러 코드. clientId 에서 요청 순서대로. */
    static Map<String, List<String>> endTxnErrors(String logs) {
        Map<String, List<String>> out = new HashMap<>();
        for (String l : logs.split("\n")) {
            if (!l.contains("Completed request:") || !l.contains("\"requestApiKeyName\":\"END_TXN\"")) {
                continue;
            }
            Matcher c = CLIENT_ID.matcher(l);
            Matcher e = RESPONSE_ERROR.matcher(l);
            if (c.find() && e.find()) {
                out.computeIfAbsent(c.group(1), k -> new ArrayList<>()).add(e.group(1));
            }
        }
        return out;
    }

    static final Pattern ROLLBACK = Pattern.compile(
            "\\[(\\d{4}-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d,\\d{3})\\] INFO \\[TransactionCoordinator id=\\d+\\] "
                    + "Completed rollback of ongoing transaction for transactionalId (\\S+) due to timeout");

    /** 브로커 로그의 시간 초과 중단 기록. transactional.id 에서 VM 시계 기준 epoch 밀리초로. */
    static Map<String, Long> rollbackTimes(String logs) {
        Map<String, Long> out = new HashMap<>();
        DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");
        Matcher m = ROLLBACK.matcher(logs);
        while (m.find()) {
            long ms = LocalDateTime.parse(m.group(1), f).toInstant(ZoneOffset.UTC).toEpochMilli();
            out.putIfAbsent(m.group(2), ms);
        }
        return out;
    }
}
