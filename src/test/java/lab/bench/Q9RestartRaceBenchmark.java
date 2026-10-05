package lab.bench;

import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lab.Containers;
import lab.lease.LeaseRepository;
import lab.store.ChatRecord;
import lab.store.CommittedReplay;
import lab.store.Q8Support;
import lab.store.Q9Support;
import lab.store.StoreFixture;
import lab.store.TxSeqWriter;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Q9 (3). 시간 초과 중단 뒤 새 producer로 재기동하는 담당자 A와, 그 사이 lease를 넘겨받는 B의 경쟁을 반복한다.
 *
 * <p>한 번: A(lease epoch 5)의 트랜잭션이 시간 초과로 중단되고, A의 commit이 실패하면 A는 producer를 닫고 lease를 확인한다
 * (기준 시각 t0, 이때 lease는 유효하다). lease는 t0 + E 에 만료되게 하고(E는 [0, 300ms)), A의 재기동(새 producer 생성과
 * init)은 t0 + dA 에 시작한다(dA는 [0, 400ms)). B는 5ms마다 lease 획득을 시도하고, 얻으면 init, 교체 표시, replay 뒤 수락한다.
 * 두 담당자 모두 트랜잭션 {@link #TXNS} 번(한 번에 1건)을 20ms 간격으로 수락하려 한다.
 *
 * <p>예외 처리는 설계 단계에 적어 둔 처리(results/Q9.md 의 처음 가설)를 따른다. commit의 {@code InvalidTxnStateException} 이면 producer를 닫고
 * lease가 유효하면 재기동, {@code InvalidProducerEpochException} 이면 abort 후 lease를 확인, abort가
 * {@code ProducerFencedException} 이면 아래 정책대로. 정책 두 가지를 교차해 네 조합을 고르게 돌린다.
 * <ul>
 *   <li>checkAfterInit: 재기동한 producer의 init 뒤, replay 전에 lease를 다시 확인한다.</li>
 *   <li>fencedChecksLease: {@code ProducerFencedException} 이면 곧바로 멈추지 않고 lease를 확인해 유효하면 재기동한다.</li>
 * </ul>
 * 재기동은 담당자마다 3번까지다.
 */
@Testcontainers
@Tag("benchmark")
class Q9RestartRaceBenchmark {

    static final int TXN_TIMEOUT_MS = 1500;
    static final String CLEANUP_INTERVAL_MS = "500";
    static final int TRIALS_PER_POLICY = 50;
    static final int TXNS = 4;
    static final long E_MAX_MS = 300;
    static final long DA_MAX_MS = 400;
    static final int MAX_RESTARTS = 3;
    static final String RAW = "q9_restart_race.csv";
    static final String SUMMARY = "q9_restart_race_summary.csv";

    @Container
    static final KafkaContainer KAFKA = Containers.kafka()
            .withEnv("KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS", CLEANUP_INTERVAL_MS)
            // 담당자별 InitProducerId, EndTxn 응답 에러 코드를 남기려고 요청 로그를 켠다.
            .withEnv("KAFKA_LOG4J_LOGGERS", "kafka.request.logger=DEBUG");

    /** 요청 로그에서 추린 줄: "시각 clientId API 에러코드". 브로커 출력을 따라가며 모은다. */
    static final java.util.concurrent.ConcurrentLinkedQueue<String> REQUESTS = new java.util.concurrent.ConcurrentLinkedQueue<>();
    static final java.util.regex.Pattern CLIENT_ID = java.util.regex.Pattern.compile("\"clientId\":\"([^\"]+)\"");
    static final java.util.regex.Pattern RESPONSE_ERROR =
            java.util.regex.Pattern.compile("\"response\":\\{\"throttleTimeMs\":\\d+,\"errorCode\":(-?\\d+)");

    static void onBrokerLine(String l) {
        if (!l.contains("Completed request:")) {
            return;
        }
        String api = l.contains("\"requestApiKeyName\":\"INIT_PRODUCER_ID\"") ? "INIT"
                : l.contains("\"requestApiKeyName\":\"END_TXN\"") ? "END_TXN" : null;
        if (api == null) {
            return;
        }
        var c = CLIENT_ID.matcher(l);
        var e = RESPONSE_ERROR.matcher(l);
        if (c.find() && e.find()) {
            String time = l.startsWith("[") ? l.substring(1, l.indexOf(']')) : "?";
            REQUESTS.add(time.substring(time.length() - 12).replace(",", ".") + " " + c.group(1) + " " + api + " " + e.group(1));
        }
    }

    @Container
    static final PostgreSQLContainer POSTGRES = Containers.postgres();

    record Policy(boolean checkAfterInit, boolean fencedChecksLease) {
        String label() {
            return (checkAfterInit ? "check_after_init" : "no_check_after_init") + "+"
                    + (fencedChecksLease ? "fenced_checks_lease" : "fenced_stops");
        }
    }

    /** 담당자 한 명(재기동을 포함한 producer들)의 진행 기록. 시각은 t0 기준 밀리초. */
    static final class Owner {
        final String base;
        final long epoch;
        final List<TxSeqWriter> writers = Collections.synchronizedList(new ArrayList<>());
        final List<String> events = Collections.synchronizedList(new ArrayList<>());
        volatile long firstInitStartMs = -1;
        volatile long firstInitEndMs = -1;
        volatile int restarts;
        volatile String stop = "done";
        volatile boolean markerPending;
        /** 성공 응답한 트랜잭션의 커밋 시작 시각들. */
        final List<long[]> ackedCommits = Collections.synchronizedList(new ArrayList<>());

        Owner(String base, long epoch) {
            this.base = base;
            this.epoch = epoch;
        }

        int acked() {
            return writers.stream().mapToInt(w -> w.ackedToUser().size()).sum();
        }

        int failed() {
            return writers.stream().mapToInt(w -> w.failedToUser().size()).sum();
        }
    }

    @Test
    void restartRace() throws Exception {
        Bench.env("Q9-3 restart race");
        KAFKA.followOutput(frame -> {
            String t = frame.getUtf8String();
            for (String l : t.split("\n")) {
                onBrokerLine(l);
            }
        });
        Bench.deleteIfExists(RAW);
        Bench.deleteIfExists(SUMMARY);
        String bootstrap = KAFKA.getBootstrapServers();
        List<Policy> policies = List.of(new Policy(false, false), new Policy(true, false), new Policy(false, true),
                new Policy(true, true));
        Random rnd = new Random(9_3026L);
        String header = "trial,policy,e_ms,da_ms,b_acquire_ms,b_init_start_ms,b_init_end_ms,a2_init_start_ms,"
                + "a2_init_end_ms,init_order,a_wake_commit_error,a_owned_at_check,a_restarts,a_stop,a_acked,a_failed,"
                + "a_acked_after_b_acquire,b_restarts,b_stop,b_acked,b_failed,b_owner_at_end,b_stopped_while_owner,"
                + "dup_seqs,gaps,acked_lost,failed_stored,log_order_rejected_acked,writers_in_log,a_events,b_events";
        Map<String, long[]> sums = new TreeMap<>();
        List<String[]> rows = new ArrayList<>();
        long t00 = System.nanoTime();
        try (HikariDataSource ds = Q8Support.dataSource(POSTGRES, null, 8);
             Admin admin = Q8Support.admin(bootstrap)) {
            StoreFixture.applySchema(ds);
            LeaseRepository leases = new LeaseRepository(ds);
            int trial = 0;
            for (int i = 0; i < TRIALS_PER_POLICY; i++) {
                for (Policy p : policies) {
                    long e = (long) (rnd.nextDouble() * E_MAX_MS);
                    long da = (long) (rnd.nextDouble() * DA_MAX_MS);
                    rows.add(trial(trial++, p, e, da, bootstrap, admin, leases, sums));
                }
                if ((i + 1) % 5 == 0) {
                    System.out.printf(Locale.ROOT, "PROGRESS trials=%d elapsed=%.1fs%n", trial,
                            (System.nanoTime() - t00) / 1e9);
                }
            }
        }
        Thread.sleep(2000);
        for (String[] r : rows) {
            String txId = r[1];
            String reqs = REQUESTS.stream().filter(x -> x.contains("@" + txId + " ")).map(x -> x.replace("@" + txId, ""))
                    .map(x -> x.replace(' ', ':')).collect(java.util.stream.Collectors.joining(">"));
            Bench.append(RAW, header + ",requests", r[0] + "," + reqs);
        }
        String sh = "policy,init_order,n,a_acked,a_acked_after_b_acquire,b_failed,a_restarts,b_restarts,"
                + "b_stopped_while_owner,dup_seqs,gaps,acked_lost,failed_stored,log_order_rejected_acked";
        sums.forEach((k, s) -> {
            StringBuilder sb = new StringBuilder(k);
            for (long x : s) {
                sb.append(',').append(x);
            }
            Bench.append(SUMMARY, sh, sb.toString());
            System.out.println("SUMMARY " + sb);
        });
    }

    String[] trial(int n, Policy p, long eMs, long daMs, String bootstrap, Admin admin, LeaseRepository leases,
                 Map<String, long[]> sums) throws Exception {
        String id = n + "-" + System.nanoTime();
        String topic = "q9-race-" + id;
        String conv = "conv-race-" + id;
        String txId = "tx-" + conv;
        StoreFixture.createTopic(bootstrap, topic);
        leases.create(conv, 4);
        Map<String, Object> cfg = Map.of(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, TXN_TIMEOUT_MS);
        long epochA = leases.tryAcquire(conv, "A", Duration.ofSeconds(60)).orElseThrow();
        Owner aOwner = new Owner("A", epochA);
        TxSeqWriter a = new TxSeqWriter("A", bootstrap, topic, conv, txId, epochA, cfg);
        aOwner.writers.add(a);
        a.initTransactions();
        a.startAt(100);
        a.begin();
        a.send(1);
        a.commit();
        a.begin();
        a.send(2);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (admin.describeTransactions(List.of(txId)).description(txId).get(10, TimeUnit.SECONDS).state()
                != TransactionState.COMPLETE_ABORT) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("시간 초과 중단이 일어나지 않았다");
            }
            Thread.sleep(50);
        }
        String wakeErr = "-";
        try {
            a.commit();
        } catch (RuntimeException ex) {
            wakeErr = ex.getClass().getSimpleName();
        }
        a.close();
        boolean ownedAtCheck = leases.isOwner(conv, epochA);
        long t0 = System.nanoTime();
        leases.expireIn(conv, epochA, Duration.ofMillis(eMs));

        Owner bOwner = new Owner("B", -1);
        long[] bAcquire = {-1};
        CompletableFuture<Owner> bFut = CompletableFuture.supplyAsync(() -> {
            try {
                long dl = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (System.nanoTime() < dl) {
                    var got = leases.tryAcquire(conv, "B", Duration.ofSeconds(60));
                    if (got.isPresent()) {
                        bAcquire[0] = ms(t0);
                        Owner b = new Owner("B", got.getAsLong());
                        b.markerPending = true;
                        run(b, true, p, bootstrap, topic, conv, txId, cfg, leases, t0);
                        return b;
                    }
                    Thread.sleep(5);
                }
                throw new IllegalStateException("B가 lease를 얻지 못했다");
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        });
        CompletableFuture<Void> aFut = CompletableFuture.runAsync(() -> {
            try {
                long wakeAt = t0 + TimeUnit.MILLISECONDS.toNanos(daMs);
                long now = System.nanoTime();
                if (wakeAt > now) {
                    TimeUnit.NANOSECONDS.sleep(wakeAt - now);
                }
                if (ownedAtCheckOk(ownedAtCheck, aOwner)) {
                    run(aOwner, false, p, bootstrap, topic, conv, txId, cfg, leases, t0);
                }
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        });
        aFut.get(60, TimeUnit.SECONDS);
        bOwner = bFut.get(60, TimeUnit.SECONDS);
        Q8Support.awaitStable(admin, topic);
        List<ChatRecord> log = CommittedReplay.readAll(bootstrap, topic, Duration.ofSeconds(20));
        List<TxSeqWriter> all = new ArrayList<>(aOwner.writers);
        all.addAll(bOwner.writers);
        Q9Support.Verdict v = Q9Support.judge(log, 100, all);
        boolean bOwnerAtEnd = leases.isOwner(conv, bOwner.epoch);
        boolean bStoppedWhileOwner = !"done".equals(bOwner.stop) && bOwnerAtEnd;
        long bAcq = bAcquire[0];
        int aAfter = (int) aOwner.ackedCommits.stream().filter(c -> c[0] >= bAcq).mapToLong(c -> c[1]).sum();
        String order;
        if (aOwner.firstInitStartMs < 0) {
            order = "a2_no_init";
        } else if (aOwner.firstInitEndMs <= bOwner.firstInitStartMs) {
            order = "a2_first";
        } else if (bOwner.firstInitEndMs <= aOwner.firstInitStartMs) {
            order = "b_first";
        } else {
            order = "overlap";
        }
        long[] s = sums.computeIfAbsent(p.label() + "," + order, k -> new long[12]);
        long[] add = {1, aOwner.acked() - 1, aAfter, bOwner.failed(), aOwner.restarts, bOwner.restarts, bStoppedWhileOwner ? 1 : 0,
                v.duplicateSeqs().size(), v.gaps().size(), v.ackedLost().size(), v.failedStored().size(),
                v.logOrderRejectedAcked().size()};
        for (int i = 0; i < add.length; i++) {
            s[i] += add[i];
        }
        return new String[] {String.format(Locale.ROOT,
                "%d,%s,%d,%d,%d,%d,%d,%d,%d,%s,%s,%s,%d,%s,%d,%d,%d,%d,%s,%d,%d,%s,%s,%d,%d,%d,%d,%d,%s,%s,%s",
                n, p.label(), eMs, daMs, bAcq, bOwner.firstInitStartMs, bOwner.firstInitEndMs, aOwner.firstInitStartMs,
                aOwner.firstInitEndMs, order, wakeErr, ownedAtCheck, aOwner.restarts, aOwner.stop,
                aOwner.acked() - 1, aOwner.failed() - 2, aAfter, bOwner.restarts, bOwner.stop, bOwner.acked(),
                bOwner.failed(), bOwnerAtEnd, bStoppedWhileOwner, v.duplicateSeqs().size(), v.gaps().size(), v.ackedLost().size(),
                v.failedStored().size(), v.logOrderRejectedAcked().size(), String.join(">", v.writersInLog()),
                String.join(">", aOwner.events), String.join(">", bOwner.events)), txId};
    }

    static boolean ownedAtCheckOk(boolean owned, Owner o) {
        if (!owned) {
            o.stop = "lease_lost_before_restart";
        }
        return owned;
    }

    /**
     * 담당자 한 명의 기동과 수락. 새 producer 생성, init, (B는 교체 표시), (정책) lease 확인, replay, 트랜잭션 TXNS번.
     * 예외가 나면 처음 가설의 처리대로 재기동하거나 멈춘다.
     */
    static void run(Owner o, boolean newOwner, Policy p, String bootstrap, String topic, String conv, String txId,
                    Map<String, Object> cfg, LeaseRepository leases, long t0) throws Exception {
        int done = 0;
        boolean first = true;
        while (true) {
            String name = (newOwner ? "B" : "A") + (o.restarts + (newOwner ? 1 : 2));
            name = name.equals("B1") ? "B" : name;
            TxSeqWriter w = new TxSeqWriter(name, bootstrap, topic, conv, txId, o.epoch, cfg);
            o.writers.add(w);
            long i0 = ms(t0);
            try {
                w.initTransactions();
            } catch (RuntimeException ex) {
                o.events.add(name + ":init_" + ex.getClass().getSimpleName());
                w.close();
                o.stop = "init_failed";
                return;
            }
            if (first) {
                o.firstInitStartMs = i0;
                o.firstInitEndMs = ms(t0);
                first = false;
            }
            o.events.add(name + ":init@" + i0);
            if (p.checkAfterInit() && !leases.isOwner(conv, o.epoch)) {
                o.events.add(name + ":not_owner_after_init");
                w.close();
                o.stop = "not_owner_after_init";
                return;
            }
            boolean replayed = false;
            String next = null;
            while (done < TXNS) {
                long firstSeq = w.nextSeq();
                long c0 = ms(t0);
                try {
                    if (o.markerPending) {
                        // 새 담당자는 init 뒤 교체 표시를 트랜잭션 하나로 쓴다. 이것도 막힐 수 있으므로 같은 처리를 받는다.
                        try {
                            w.writeMarker();
                        } catch (java.util.concurrent.ExecutionException ee) {
                            throw (Exception) ee.getCause();
                        }
                        o.markerPending = false;
                        o.events.add(name + ":marker@" + c0);
                        continue;
                    }
                    if (!replayed) {
                        CommittedReplay.Result rp = CommittedReplay.replay(bootstrap, topic, name + "-replay-" + txId,
                                Duration.ofSeconds(20));
                        w.startAt(rp.lastSeq() + 1);
                        replayed = true;
                        continue;
                    }
                    w.begin();
                    w.send(1);
                    w.commit();
                    o.ackedCommits.add(new long[] {c0, 1});
                    o.events.add(name + ":ok" + firstSeq);
                    done++;
                    Thread.sleep(20);
                    continue;
                } catch (InvalidTxnStateException ex) {
                    o.events.add(name + ":ITS@" + c0);
                    next = "restart_if_owner";
                } catch (InvalidProducerEpochException ex) {
                    o.events.add(name + ":IPE@" + c0);
                    try {
                        w.abort();
                        if (leases.isOwner(conv, o.epoch)) {
                            o.events.add(name + ":abort_ok_owner");
                            w.rewindTo(firstSeq);
                            done++;
                            continue;
                        }
                        o.events.add(name + ":abort_ok_not_owner");
                        next = "stop";
                    } catch (ProducerFencedException pf) {
                        o.events.add(name + ":abort_PFE");
                        next = p.fencedChecksLease() ? "restart_if_owner" : "stop";
                    } catch (RuntimeException other) {
                        o.events.add(name + ":abort_" + other.getClass().getSimpleName());
                        next = "restart_if_owner";
                    }
                } catch (ProducerFencedException ex) {
                    o.events.add(name + ":PFE@" + c0);
                    next = p.fencedChecksLease() ? "restart_if_owner" : "stop";
                } catch (Exception ex) {
                    Throwable c = ex.getCause() != null && ex instanceof java.util.concurrent.ExecutionException
                            ? ex.getCause() : ex;
                    o.events.add(name + ":" + c.getClass().getSimpleName() + "@" + c0);
                    next = "restart_if_owner";
                }
                break;
            }
            w.close();
            if (next == null) {
                o.stop = "done";
                return;
            }
            if (next.equals("stop")) {
                o.stop = "stopped_on_fence";
                return;
            }
            if (!leases.isOwner(conv, o.epoch)) {
                o.events.add(name + ":not_owner");
                o.stop = "not_owner";
                return;
            }
            if (o.restarts >= MAX_RESTARTS) {
                o.stop = "restart_limit";
                return;
            }
            o.restarts++;
            done++;
        }
    }

    static long ms(long t0) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
    }
}
