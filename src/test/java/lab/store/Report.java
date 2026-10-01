package lab.store;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Q5 테스트가 관찰한 것을 순서대로 적는 기록. lab.kafka.Report와 같이 표준 출력과
 * build/lab-output/&lt;이름&gt;.txt 에 함께 남긴다.
 */
public final class Report {

    private final String name;
    private final StringBuilder sb = new StringBuilder();

    public Report(String name) {
        this.name = name;
    }

    public void line(String s) {
        sb.append(s).append('\n');
        System.out.println(s);
    }

    public void printf(String format, Object... args) {
        String s = String.format(format, args);
        String body = s.endsWith("\n") ? s.substring(0, s.length() - 1) : s;
        line(body);
    }

    public void save() throws IOException {
        Path dir = Path.of("build", "lab-output");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(name + ".txt"), sb.toString(), StandardCharsets.UTF_8);
    }
}
