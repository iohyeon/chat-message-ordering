package lab.bench;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lab.Containers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Q8 (4). 한 브로커에 트랜잭션 producer를 64, 256, 1,024개 두고 producer마다 초당 약 57건이 들어올 때, 커밋 주기
 * 10ms, 40ms, 100ms에 따라 커밋 수, 트랜잭션당 레코드 수, 커밋 지연, 수락 지연, 브로커 CPU가 어떻게 바뀌는가.
 *
 * <p>producer는 {@link Q8ProducerScaleClient} 가 브로커와 같은 Docker 네트워크의 컨테이너 안에서 연다. 컨테이너는 브로커 이미지에
 * 든 JRE로, 호스트에서 빌드한 클래스와 의존성 jar를 같은 경로로 마운트해 실행한다. 호스트는 그동안 브로커 컨테이너의 cgroup
 * {@code cpu.stat}(usage_usec)을 0.5초마다 읽고, 클라이언트가 찍은 측정 구간과 기준선 구간의 브로커 CPU를 보간해 계산한다.
 * 클라이언트와 브로커가 같은 VM(CPU 4개)을 나눠 쓰므로 브로커 CPU는 경향으로만 쓴다.
 */
@Tag("benchmark")
class Q8ProducerScaleBenchmark {

    static final String FILE = "q8_4_producer_scale.csv";

    record CpuSample(long millis, long usageMicros) {
    }

    @Test
    void scale() throws Exception {
        Bench.env("Q8-4 host");
        Bench.deleteIfExists(FILE);
        String home = System.getProperty("user.home");
        Path build = Path.of("build").toAbsolutePath();
        Path gradleCaches = Path.of(home, ".gradle", "caches");
        String cp = java.util.Arrays.stream(System.getProperty("java.class.path").split(File.pathSeparator))
                .filter(e -> e.startsWith(build.toString()) || e.startsWith(gradleCaches.toString()))
                .collect(Collectors.joining(":"));
        ConcurrentLinkedQueue<String> out = new ConcurrentLinkedQueue<>();
        List<CpuSample> cpu = java.util.Collections.synchronizedList(new ArrayList<>());
        ConcurrentLinkedQueue<String> pending = new ConcurrentLinkedQueue<>();
        try (Network net = Network.newNetwork();
             KafkaContainer kafka = Containers.kafka().withNetwork(net).withNetworkAliases("kafka")
                     .withListener("kafka:19092")) {
            kafka.start();
            AtomicBoolean stop = new AtomicBoolean();
            // 표본을 읽을 때마다, 측정 구간이 표본 범위 안에 들어온 RESULT 줄을 바로 CSV로 쓴다. 실행이 중간에 멈춰도 앞의 행은 남는다.
            Thread poller = new Thread(() -> {
                while (!stop.get()) {
                    long u = brokerCpuMicros(kafka);
                    if (u >= 0) {
                        cpu.add(new CpuSample(System.currentTimeMillis(), u));
                        flushRows(pending, cpu);
                    }
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }, "cpu-poller");
            poller.setDaemon(true);
            poller.start();
            try (GenericContainer<?> client = new GenericContainer<>(Containers.KAFKA_IMAGE)
                    .withNetwork(net)
                    .withFileSystemBind(build.toString(), build.toString(), BindMode.READ_ONLY)
                    .withFileSystemBind(gradleCaches.toString(), gradleCaches.toString(), BindMode.READ_ONLY)
                    .withCreateContainerCmdModifier(c -> c.withEntrypoint("java"))
                    .withCommand("-Xmx2g", "-cp", cp, Q8ProducerScaleClient.class.getName(), "kafka:19092",
                            "64,256,1024", "10,40,100", "2")
                    .withLogConsumer(f -> {
                        String s = f.getUtf8String().stripTrailing();
                        System.out.println("[client] " + s);
                        out.add(s);
                        if (s.startsWith("RESULT ")) {
                            pending.add(s);
                        }
                    })
                    .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofMinutes(40)))) {
                client.start();
            } finally {
                Thread.sleep(1_500);
                stop.set(true);
                poller.join(10_000);
            }
        }
        boolean done = out.stream().anyMatch(l -> l.contains("CLIENT-DONE"));
        if (!done || ROWS.get() == 0 || !pending.isEmpty()) {
            throw new IllegalStateException("클라이언트가 끝나지 않았거나 CPU를 계산하지 못한 행이 있다. rows=" + ROWS.get()
                    + " pending=" + pending.size());
        }
    }

    static final java.util.concurrent.atomic.AtomicInteger ROWS = new java.util.concurrent.atomic.AtomicInteger();

    /** CPU 표본이 측정 구간 끝을 넘어선 RESULT 줄을 CSV 행으로 바꿔 쓴다. */
    static void flushRows(ConcurrentLinkedQueue<String> pending, List<CpuSample> cpu) {
        List<CpuSample> snap;
        synchronized (cpu) {
            snap = new ArrayList<>(cpu);
        }
        if (snap.isEmpty()) {
            return;
        }
        long last = snap.get(snap.size() - 1).millis();
        for (var it = pending.iterator(); it.hasNext(); ) {
            String l = it.next();
            String[] f = l.substring(7).split(",", -1);
            long idleS = Long.parseLong(f[17]), idleE = Long.parseLong(f[18]);
            long winS = Long.parseLong(f[19]), winE = Long.parseLong(f[20]);
            if (winE > last) {
                continue;
            }
            long winCommits = Long.parseLong(f[21]);
            double idleCores = cores(snap, idleS, idleE);
            double brokerCores = cores(snap, winS, winE);
            double perCommitUs = winCommits == 0 ? Double.NaN
                    : (brokerCores - idleCores) * (winE - winS) * 1000.0 / winCommits;
            String line = String.join(",", java.util.Arrays.copyOfRange(f, 0, 17))
                    + String.format(Locale.ROOT, ",%.2f,%.2f,%.1f,%d", idleCores, brokerCores, perCommitUs, winE - winS);
            System.out.println("ROW " + line);
            Bench.append(FILE, "producers,commit_ms,rep,offered_records_per_sec,committed_records_per_sec,commits_per_sec,"
                    + "commit_upper_bound_per_sec,records_per_tx_mean,records_per_tx_p50,commit_call_p50_ms,"
                    + "commit_call_p99_ms,accept_p50_ms,accept_p99_ms,accept_max_ms,client_cpu_cores,errors,error_kinds,"
                    + "broker_idle_cpu_cores,broker_cpu_cores,broker_cpu_us_per_commit_over_idle,window_ms", line);
            ROWS.incrementAndGet();
            it.remove();
        }
    }

    /** [from, to] 구간의 브로커 CPU 코어 수. 표본 사이는 선형 보간한다. */
    static double cores(List<CpuSample> s, long from, long to) {
        return (usageAt(s, to) - usageAt(s, from)) / 1e3 / (to - from);
    }

    static double usageAt(List<CpuSample> s, long t) {
        for (int i = 1; i < s.size(); i++) {
            CpuSample a = s.get(i - 1), b = s.get(i);
            if (a.millis() <= t && t <= b.millis()) {
                return a.usageMicros() + (double) (b.usageMicros() - a.usageMicros()) * (t - a.millis())
                        / Math.max(1, b.millis() - a.millis());
            }
        }
        throw new IllegalStateException("CPU 표본 범위 밖의 시각: " + t);
    }

    /** 브로커 컨테이너 cgroup의 누적 CPU 시간(µs). 2초 안에 읽지 못하면 -1(호스트가 멈췄을 때 exec가 돌아오지 않은 적이 있다). */
    static long brokerCpuMicros(KafkaContainer kafka) {
        try {
            var r = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                try {
                    return kafka.execInContainer("cat", "/sys/fs/cgroup/cpu.stat");
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }).get(2, java.util.concurrent.TimeUnit.SECONDS);
            for (String l : r.getStdout().split("\n")) {
                if (l.startsWith("usage_usec ")) {
                    return Long.parseLong(l.substring("usage_usec ".length()).trim());
                }
            }
        } catch (Exception e) {
            return -1;
        }
        return -1;
    }
}
