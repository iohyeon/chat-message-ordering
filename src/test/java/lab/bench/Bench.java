package lab.bench;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Locale;
import java.util.zip.GZIPOutputStream;

/** 측정 공통 도구: 분위수, CSV 기록, GC 계수. */
final class Bench {

    static final Path DATA = Path.of("results", "data");

    private Bench() {
    }

    /** 분위수 요약. 입력 단위는 나노초, 출력은 밀리초. */
    record Summary(int n, double p50, double p90, double p99, double max, double mean) {
        static Summary of(long[] nanos) {
            if (nanos.length == 0) {
                return new Summary(0, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
            }
            long[] s = nanos.clone();
            Arrays.sort(s);
            double mean = Arrays.stream(s).average().orElse(Double.NaN) / 1e6;
            return new Summary(s.length, pct(s, 0.50), pct(s, 0.90), pct(s, 0.99), s[s.length - 1] / 1e6, mean);
        }

        /** nearest-rank 방식. */
        static double pct(long[] sorted, double q) {
            int idx = (int) Math.ceil(q * sorted.length) - 1;
            return sorted[Math.max(0, Math.min(sorted.length - 1, idx))] / 1e6;
        }

        String csv() {
            return String.format(Locale.ROOT, "%d,%.3f,%.3f,%.3f,%.3f,%.3f", n, p50, p90, p99, max, mean);
        }

        static final String HEADER = "n,p50_ms,p90_ms,p99_ms,max_ms,mean_ms";
    }

    /** 이 JVM의 GC 누적 횟수와 시간(ms). 측정 구간 전후 차이를 기록한다. */
    record Gc(long count, long millis) {
        static Gc now() {
            long c = 0, t = 0;
            for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
                c += Math.max(0, b.getCollectionCount());
                t += Math.max(0, b.getCollectionTime());
            }
            return new Gc(c, t);
        }

        Gc minus(Gc o) {
            return new Gc(count - o.count, millis - o.millis);
        }
    }

    /** 헤더가 없으면 쓰고, 행을 덧붙인다. */
    static synchronized void append(String file, String header, String line) {
        try {
            Files.createDirectories(DATA);
            Path p = DATA.resolve(file);
            boolean fresh = !Files.exists(p);
            try (var w = Files.newBufferedWriter(p, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND)) {
                if (fresh) {
                    w.write(header);
                    w.newLine();
                }
                w.write(line);
                w.newLine();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 원 표본을 gzip CSV로 쓴다. */
    static PrintWriter gzipWriter(String file, String header) {
        try {
            Files.createDirectories(DATA);
            var out = new GZIPOutputStream(Files.newOutputStream(DATA.resolve(file)));
            var pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8)));
            pw.println(header);
            return pw;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void deleteIfExists(String file) {
        try {
            Files.deleteIfExists(DATA.resolve(file));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void env(String title) {
        Runtime rt = Runtime.getRuntime();
        System.out.printf("ENV %s: jvm=%s availableProcessors=%d maxHeapMB=%d gc=%s%n", title,
                System.getProperty("java.vm.version"), rt.availableProcessors(), rt.maxMemory() / (1024 * 1024),
                ManagementFactory.getGarbageCollectorMXBeans().stream().map(GarbageCollectorMXBean::getName).toList());
    }
}
