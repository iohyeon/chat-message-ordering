package lab.kafka;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lab.shard.ShardProducer.CallbackResult;
import lab.shard.ShardProducer.Sent;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 실험 중 관찰한 것을 순서대로 적는 기록. 표준 출력과 build/lab-output/&lt;이름&gt;.txt 에 함께 남긴다.
 */
final class Report {

    private final String name;
    private final StringBuilder sb = new StringBuilder();

    Report(String name) {
        this.name = name;
    }

    void line(String s) {
        sb.append(s).append('\n');
        System.out.println(s);
    }

    void section(String title) {
        line("");
        line("=== " + title + " ===");
    }

    void lines(List<String> ls) {
        ls.forEach(l -> line("  " + l));
    }

    void save() throws IOException {
        Path dir = Path.of("build", "lab-output");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(name + ".txt"), sb.toString(), StandardCharsets.UTF_8);
    }

    // ---------------------------------------------------------------- 호출 결과 기록

    /** 한 호출의 결과. ok면 exceptionClass가 null이다. */
    record Outcome(String step, String exceptionClass, String message, List<String> causeChain, long millis) {
        boolean ok() {
            return exceptionClass == null;
        }

        String render() {
            if (ok()) {
                return step + " -> 성공 (" + millis + "ms)";
            }
            return step + " -> " + exceptionClass + ": " + message + " (" + millis + "ms)"
                    + (causeChain.size() > 1 ? "\n      원인 체인: " + String.join(" <- ", causeChain) : "");
        }
    }

    interface ThrowingRunnable {
        void run() throws Exception;
    }

    Outcome call(String step, ThrowingRunnable r) {
        long t0 = System.nanoTime();
        Outcome o;
        try {
            r.run();
            o = new Outcome(step, null, null, List.of(), ms(t0));
        } catch (Throwable t) {
            o = new Outcome(step, t.getClass().getName(), t.getMessage(), chain(t), ms(t0));
        }
        line("  " + o.render());
        return o;
    }

    /** send() 의 Future와 콜백 결과를 따로 기록한다. */
    record SendOutcome(Outcome future, Outcome callback, RecordMetadata metadata, String callbackThread) {
    }

    SendOutcome sendResult(String step, Sent sent, long waitSeconds) {
        long t0 = System.nanoTime();
        Outcome future;
        RecordMetadata md = null;
        try {
            md = sent.future().get(waitSeconds, TimeUnit.SECONDS);
            future = new Outcome(step + " Future.get()", null, null, List.of(), ms(t0));
        } catch (ExecutionException e) {
            Throwable c = e.getCause();
            future = new Outcome(step + " Future.get()", c.getClass().getName(), c.getMessage(), chain(c), ms(t0));
        } catch (TimeoutException e) {
            future = new Outcome(step + " Future.get()", "TIMEOUT", waitSeconds + "초 안에 끝나지 않음", List.of(), ms(t0));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        Outcome callback;
        String thread = null;
        try {
            CallbackResult cb = sent.callback().get(waitSeconds, TimeUnit.SECONDS);
            thread = cb.thread();
            if (cb.exception() == null) {
                callback = new Outcome(step + " callback", null, null, List.of(), 0);
            } else {
                callback = new Outcome(step + " callback", cb.exception().getClass().getName(),
                        cb.exception().getMessage(), chain(cb.exception()), 0);
            }
        } catch (Exception e) {
            callback = new Outcome(step + " callback", "TIMEOUT", "콜백이 불리지 않음", List.of(), 0);
        }
        line("  " + future.render() + (md != null ? " offset=" + md.offset() : ""));
        line("  " + callback.render() + (thread != null ? " [콜백 스레드=" + thread + "]" : ""));
        return new SendOutcome(future, callback, md, thread);
    }

    private static long ms(long t0) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
    }

    static List<String> chain(Throwable t) {
        List<String> out = new ArrayList<>();
        for (Throwable c = t; c != null && out.size() < 6; c = c.getCause()) {
            out.add(c.getClass().getSimpleName() + "(" + c.getMessage() + ")");
            if (c.getCause() == c) {
                break;
            }
        }
        return out;
    }
}
