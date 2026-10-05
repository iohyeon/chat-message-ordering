package lab.bench;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Q8 (4)의 producer 쪽. 브로커와 같은 Docker 네트워크의 컨테이너 안에서 돈다({@link Q8ProducerScaleBenchmark} 가 띄운다).
 * 호스트에서 producer 1,024개를 열면 colima의 SSH 포트 포워딩이 연결을 모두 끊었으므로, 포워딩을 거치지 않게 했다.
 *
 * <p>producer 하나가 virtual shard 하나다. 레코드는 producer마다 포아송 도착(평균 초당 57건)으로 정한 예정 시각에 보낸다.
 * 트랜잭션은 레코드가 처음 올 때 열고, 연 지 커밋 주기가 지나면 커밋한다. 빈 트랜잭션은 커밋하지 않는다(클라이언트는
 * 보낸 것이 없는 트랜잭션의 EndTxn을 보내지 않는다). producer마다 가상 스레드 하나가 보내기와 커밋을 하고, 커밋 중에
 * 도착한 레코드는 커밋이 끝난 뒤 보낸다. 수락 지연은 예정 시각부터 그 레코드가 든 트랜잭션의 commitTransaction() 이
 * 돌아온 시각까지다. 표준 출력에 측정 구간의 벽시계 시각을 함께 찍어, 호스트가 같은 구간의 브로커 CPU를 계산하게 한다.
 */
public final class Q8ProducerScaleClient {

    static final double RATE_PER_PRODUCER = 57.0;
    static final long WARMUP_MS = 3_000;
    static final long MEASURE_MS = 10_000;
    static final long IDLE_MS = 2_000;
    static final int PARTITIONS = 32;

    record TxStat(long commitEndNanos, int records, long commitNanos) {
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = args[0];
        int[] producers = Arrays.stream(args[1].split(",")).mapToInt(Integer::parseInt).toArray();
        int[] commitMs = Arrays.stream(args[2].split(",")).mapToInt(Integer::parseInt).toArray();
        int repeats = Integer.parseInt(args[3]);
        System.out.println("CLIENT jvm=" + System.getProperty("java.vm.version") + " cpus="
                + Runtime.getRuntime().availableProcessors() + " maxHeapMB=" + Runtime.getRuntime().maxMemory() / (1 << 20));
        for (int rep = 1; rep <= repeats; rep++) {
            for (int n : producers) {
                runProducers(bootstrap, n, commitMs, rep);
            }
        }
        System.out.println("CLIENT-DONE");
    }

    static void runProducers(String bootstrap, int n, int[] commitMsList, int rep) throws Exception {
        String topic = "q8-scale-" + n + "-" + rep + "-" + System.nanoTime();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.createTopics(List.of(new NewTopic(topic, Optional.of(PARTITIONS), Optional.empty()))).all().get();
        }
        List<KafkaProducer<String, byte[]>> producers = new ArrayList<>();
        long c0 = System.nanoTime();
        try (ExecutorService vt = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < n; i++) {
                producers.add(new KafkaProducer<>(Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.LINGER_MS_CONFIG, 0,
                        ProducerConfig.CLIENT_ID_CONFIG, "scale-" + n + "-" + i,
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG, "scale-" + n + "-" + rep + "-" + i + "-" + System.nanoTime())));
            }
            for (int from = 0; from < n; from += 64) {
                List<Future<?>> inits = new ArrayList<>();
                for (var p : producers.subList(from, Math.min(n, from + 64))) {
                    inits.add(vt.submit(() -> {
                        p.initTransactions();
                        return null;
                    }));
                }
                for (var f : inits) {
                    f.get(300, TimeUnit.SECONDS);
                }
            }
        }
        System.out.printf(Locale.ROOT, "SETUP producers=%d init=%.1fs%n", n, (System.nanoTime() - c0) / 1e9);
        try {
            for (int commitMs : commitMsList) {
                runConfig(topic, producers, commitMs, rep);
            }
        } finally {
            try (ExecutorService vt = Executors.newVirtualThreadPerTaskExecutor()) {
                for (var p : producers) {
                    vt.submit(() -> p.close(java.time.Duration.ofSeconds(5)));
                }
            }
        }
    }

    static void runConfig(String topic, List<KafkaProducer<String, byte[]>> producers, int commitMs, int rep)
            throws Exception {
        int n = producers.size();
        long idleStartMs = System.currentTimeMillis();
        Thread.sleep(IDLE_MS);
        long idleEndMs = System.currentTimeMillis();
        var os = (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        long start = System.nanoTime() + 200_000_000L;
        long measureStart = start + TimeUnit.MILLISECONDS.toNanos(WARMUP_MS);
        long end = measureStart + TimeUnit.MILLISECONDS.toNanos(MEASURE_MS);
        long commitNs = TimeUnit.MILLISECONDS.toNanos(commitMs);
        ConcurrentLinkedQueue<TxStat> txs = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<long[]> accepts = new ConcurrentLinkedQueue<>();
        AtomicLong errors = new AtomicLong();
        ConcurrentLinkedQueue<String> errorKinds = new ConcurrentLinkedQueue<>();
        byte[] value = new byte[100];
        long windowStartMs;
        long client0;
        long winT0;
        try (ExecutorService vt = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < n; i++) {
                final int idx = i;
                final KafkaProducer<String, byte[]> p = producers.get(i);
                vt.submit(() -> {
                    Random rnd = new Random(idx * 31L + rep * 7L + commitMs);
                    long next = start + (long) (-Math.log(1 - rnd.nextDouble()) / RATE_PER_PRODUCER * 1e9);
                    boolean open = false;
                    long openedAt = 0;
                    List<Long> pending = new ArrayList<>();
                    long[] acc = new long[4096];
                    int accN = 0;
                    try {
                        while (true) {
                            long now = System.nanoTime();
                            if (open && now - openedAt >= commitNs) {
                                long a = System.nanoTime();
                                p.commitTransaction();
                                long b = System.nanoTime();
                                txs.add(new TxStat(b, pending.size(), b - a));
                                for (long sch : pending) {
                                    if (sch >= measureStart && sch < end) {
                                        if (accN == acc.length) {
                                            acc = Arrays.copyOf(acc, acc.length * 2);
                                        }
                                        acc[accN++] = b - sch;
                                    }
                                }
                                pending.clear();
                                open = false;
                                continue;
                            }
                            if (next >= end && !open) {
                                break;
                            }
                            if (next < end && now >= next) {
                                if (!open) {
                                    p.beginTransaction();
                                    open = true;
                                    openedAt = System.nanoTime();
                                }
                                p.send(new ProducerRecord<>(topic, idx % PARTITIONS, "shard-" + idx, value));
                                pending.add(next);
                                next += (long) (-Math.log(1 - rnd.nextDouble()) / RATE_PER_PRODUCER * 1e9);
                                continue;
                            }
                            long wake = next < end ? next : Long.MAX_VALUE;
                            if (open) {
                                wake = Math.min(wake, openedAt + commitNs);
                            }
                            LockSupport.parkNanos(Math.max(0, wake - System.nanoTime()));
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                        errorKinds.add(e.getClass().getSimpleName());
                    }
                    accepts.add(Arrays.copyOf(acc, accN));
                    return null;
                });
            }
            parkUntil(measureStart);
            windowStartMs = System.currentTimeMillis();
            client0 = os.getProcessCpuTime();
            winT0 = System.nanoTime();
            parkUntil(end);
        }
        // 측정 구간: measureStart부터 모든 producer가 마지막 커밋을 끝낼 때까지. 커밋 수도 같은 구간으로 센다.
        long windowEndMs = System.currentTimeMillis();
        long client1 = os.getProcessCpuTime();
        long winT1 = System.nanoTime();
        long commits = 0, recs = 0, winCommits = 0, winRecs = 0;
        List<Long> commitLat = new ArrayList<>();
        List<Long> perTx = new ArrayList<>();
        for (TxStat t : txs) {
            if (t.commitEndNanos() >= measureStart && t.commitEndNanos() < end) {
                commits++;
                recs += t.records();
                commitLat.add(t.commitNanos());
                perTx.add((long) t.records());
            }
            if (t.commitEndNanos() >= winT0 && t.commitEndNanos() <= winT1) {
                winCommits++;
                winRecs += t.records();
            }
        }
        long total = accepts.stream().mapToLong(a -> a.length).sum();
        long[] all = new long[(int) total];
        int k = 0;
        for (long[] a : accepts) {
            System.arraycopy(a, 0, all, k, a.length);
            k += a.length;
        }
        double sec = MEASURE_MS / 1000.0;
        double winSec = (winT1 - winT0) / 1e9;
        long[] pt = perTx.stream().mapToLong(Long::longValue).sorted().toArray();
        Bench.Summary cl = Bench.Summary.of(commitLat.stream().mapToLong(Long::longValue).toArray());
        Bench.Summary ac = Bench.Summary.of(all);
        // 열: producers,commit_ms,rep,offered,committed_rps,commits_ps,upper,rec_per_tx_mean,rec_per_tx_p50,
        // commit_p50,commit_p99,accept_p50,accept_p99,accept_max,client_cores,errors,error_kinds,
        // idle_start_ms,idle_end_ms,window_start_ms,window_end_ms,window_commits,window_records
        System.out.printf(Locale.ROOT, "RESULT %d,%d,%d,%.1f,%.1f,%.1f,%.1f,%.2f,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.2f,%d,%s,"
                        + "%d,%d,%d,%d,%d,%d%n",
                n, commitMs, rep, n * RATE_PER_PRODUCER, recs / sec, commits / sec, n * 1000.0 / commitMs,
                commits == 0 ? 0 : (double) recs / commits, pt.length == 0 ? 0 : pt[pt.length / 2], cl.p50(), cl.p99(),
                ac.p50(), ac.p99(), ac.max(), (client1 - client0) / 1e9 / winSec, errors.get(),
                errorKinds.stream().distinct().reduce((x, y) -> x + "|" + y).orElse(""),
                idleStartMs, idleEndMs, windowStartMs, windowEndMs, winCommits, winRecs);
    }

    static void parkUntil(long t) {
        long w;
        while ((w = t - System.nanoTime()) > 0) {
            LockSupport.parkNanos(Math.min(w, 1_000_000));
        }
    }
}
