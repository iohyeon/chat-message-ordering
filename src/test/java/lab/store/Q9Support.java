package lab.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Q9 판정 코드. 결과 판정의 기준은 {@code read_committed} 로그다(유일 키와 {@code ON CONFLICT} 만 둔 저장소가 그대로
 * 옮겨 담는 내용과 같다, Q8).
 */
public final class Q9Support {

    private Q9Support() {
    }

    /**
     * 한 실행의 판정.
     *
     * @param duplicateSeqs        read_committed 로그에서 두 번 이상 나온 순번
     * @param gaps                 첫 순번부터 가장 큰 순번 사이의 빈 순번
     * @param ackedLost            성공 응답했지만 로그에 없는 레코드
     * @param failedStored         실패 응답했지만 로그에 있는 레코드
     * @param brokerAckedAborted   브로커 ack는 받았지만 로그에 없는 레코드(ack 시점에 응답했다면 사라졌을 것)
     * @param logOrderRejectedAcked 성공 응답한 레코드 중, 로그 순서 기준(LOG_ORDER)으로 비교하면 거부될 것. 앞선 교체 표시의
     *                             epoch보다 작은 epoch를 단 레코드다
     * @param writersInLog         로그에 커밋된 레코드를 쓴 담당자 이름, 로그 순서대로(같은 이름이 이어지면 한 번)
     */
    public record Verdict(Set<Long> duplicateSeqs, List<Long> gaps, List<ChatRecord> ackedLost,
                          List<ChatRecord> failedStored, List<ChatRecord> brokerAckedAborted,
                          List<ChatRecord> logOrderRejectedAcked, List<String> writersInLog) {

        public String render() {
            return "순번 겹침=" + duplicateSeqs + " 빈자리=" + gaps
                    + " 성공 응답 유실=" + bodies(ackedLost) + " 실패 응답 저장=" + bodies(failedStored)
                    + " 브로커 ack 뒤 중단=" + bodies(brokerAckedAborted)
                    + " LOG_ORDER였다면 거부될 성공 응답=" + bodies(logOrderRejectedAcked)
                    + " 로그의 담당자 순서=" + writersInLog;
        }
    }

    public static Verdict judge(List<ChatRecord> committedLog, long firstSeq, Collection<TxSeqWriter> writers) {
        List<ChatRecord> acked = new ArrayList<>();
        List<ChatRecord> failed = new ArrayList<>();
        List<ChatRecord> brokerAcked = new ArrayList<>();
        for (TxSeqWriter w : writers) {
            acked.addAll(w.ackedToUser());
            failed.addAll(w.failedToUser());
            brokerAcked.addAll(w.brokerAcked());
        }
        return judge(committedLog, firstSeq, acked, failed, brokerAcked);
    }

    public static Verdict judge(List<ChatRecord> committedLog, long firstSeq, List<ChatRecord> acked,
                                List<ChatRecord> failed, List<ChatRecord> brokerAcked) {
        Map<Long, List<String>> dup = Q8Support.duplicateSeqsInLog(committedLog);
        Set<String> inLog = committedLog.stream().filter(r -> !r.marker()).map(ChatRecord::body)
                .collect(Collectors.toSet());
        TreeSet<Long> seqs = committedLog.stream().filter(r -> !r.marker()).map(ChatRecord::seq)
                .collect(Collectors.toCollection(TreeSet::new));
        List<Long> gaps = new ArrayList<>();
        if (!seqs.isEmpty()) {
            for (long s = firstSeq; s <= seqs.last(); s++) {
                if (!seqs.contains(s)) {
                    gaps.add(s);
                }
            }
        }
        Set<String> rejectedByLogOrder = new java.util.HashSet<>();
        long fence = Long.MIN_VALUE;
        for (ChatRecord r : committedLog) {
            if (r.marker()) {
                fence = Math.max(fence, r.epoch());
            } else if (r.epoch() < fence) {
                rejectedByLogOrder.add(r.body());
            }
        }
        List<String> writers = new ArrayList<>();
        for (ChatRecord r : committedLog) {
            String w = r.marker() ? "MARKER(e" + r.epoch() + ")" : r.body().substring(0, r.body().indexOf('-'));
            if (writers.isEmpty() || !writers.get(writers.size() - 1).equals(w)) {
                writers.add(w);
            }
        }
        return new Verdict(dup.keySet(), gaps,
                acked.stream().filter(r -> !inLog.contains(r.body())).toList(),
                failed.stream().filter(r -> inLog.contains(r.body())).toList(),
                brokerAcked.stream().filter(r -> !inLog.contains(r.body())).toList(),
                acked.stream().filter(r -> rejectedByLogOrder.contains(r.body())).toList(),
                writers);
    }

    public static List<String> bodies(List<ChatRecord> rs) {
        return rs.stream().map(ChatRecord::body).toList();
    }

    /** 로그 한 줄. 교체 표시는 epoch만 적는다. */
    public static String line(ChatRecord r) {
        return r.marker() ? "MARKER epoch=" + r.epoch() : "seq=" + r.seq() + " epoch=" + r.epoch() + " body=" + r.body();
    }
}
