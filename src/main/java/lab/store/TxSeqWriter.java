package lab.store;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lab.shard.ShardProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Q9. 트랜잭션 producer 하나로 순번을 매기는 담당자. {@link TxEpochWriter} 와 같은 레코드 형식({@link ChatRecord})을 쓰지만,
 * 트랜잭션을 여는 것, 보내는 것, 커밋, 중단을 따로 부를 수 있고 레코드마다 무엇을 응답했는지를 셋으로 나눠 기록한다.
 *
 * <ul>
 *   <li>브로커 ack: {@code send()} 의 Future가 성공한 레코드. 커밋 전이므로 사용자에게 응답한 것이 아니다.</li>
 *   <li>성공 응답: {@code commitTransaction()} 이 돌아온 트랜잭션의 레코드.</li>
 *   <li>실패 응답: 커밋이나 중단이 예외로 끝났거나, 중단했거나, producer를 버린 트랜잭션의 레코드.</li>
 * </ul>
 *
 * <p>본문은 {@code <이름>-<순번>#<시도 번호>} 이다. 같은 담당자가 같은 순번을 다시 보내도 본문이 달라서, 중단된 것과
 * 커밋된 것을 로그에서 구별할 수 있다.
 */
public final class TxSeqWriter implements AutoCloseable {

    private final String name;
    private final String topic;
    private final String conversationId;
    private final long epoch;
    private final KafkaProducer<String, String> producer;
    private long nextSeq;
    private int attempt;
    private final List<ChatRecord> pending = new ArrayList<>();
    private final List<ChatRecord> brokerAcked = Collections.synchronizedList(new ArrayList<>());
    private final List<ChatRecord> ackedToUser = Collections.synchronizedList(new ArrayList<>());
    private final List<ChatRecord> failedToUser = Collections.synchronizedList(new ArrayList<>());

    public TxSeqWriter(String name, String bootstrap, String topic, String conversationId, String transactionalId,
                       long epoch, Map<String, Object> overrides) {
        this.name = name;
        this.topic = topic;
        this.conversationId = conversationId;
        this.epoch = epoch;
        Map<String, Object> cfg = new HashMap<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId,
                ProducerConfig.CLIENT_ID_CONFIG, name + "@" + transactionalId));
        cfg.putAll(overrides);
        this.producer = new KafkaProducer<>(cfg);
    }

    public void initTransactions() {
        producer.initTransactions();
    }

    public void startAt(long seq) {
        this.nextSeq = seq;
    }

    /** 다음에 매길 순번을 되돌린다. 중단한 트랜잭션의 순번을 다시 쓰려 할 때 부른다. */
    public void rewindTo(long seq) {
        this.nextSeq = seq;
    }

    public void begin() {
        producer.beginTransaction();
    }

    /**
     * 열린 트랜잭션에 레코드 {@code n} 개를 보내고 {@code flush()} 한다. 보낸 레코드는 곧바로 열린 트랜잭션의 레코드가 되므로,
     * 일부가 실패해도 뒤의 커밋이나 중단에서 실패 응답으로 정리된다. 실패한 Future가 있으면 그 원인을 던진다.
     */
    public List<ChatRecord> send(int n) throws Exception {
        List<ChatRecord> sent = new ArrayList<>();
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            long seq = nextSeq++;
            ChatRecord r = ChatRecord.message(conversationId, seq, epoch, name + "-" + seq + "#" + (++attempt));
            pending.add(r);
            sent.add(r);
            futures.add(producer.send(r.toProducerRecord(topic)));
        }
        producer.flush();
        Exception first = null;
        for (int i = 0; i < futures.size(); i++) {
            try {
                futures.get(i).get();
                brokerAcked.add(sent.get(i));
            } catch (ExecutionException e) {
                if (first == null) {
                    first = e.getCause() instanceof Exception ex ? ex : e;
                }
            }
        }
        if (first != null) {
            throw first;
        }
        return sent;
    }

    /** 커밋한다. 돌아오면 열린 트랜잭션의 레코드를 성공 응답, 예외면 실패 응답으로 기록하고 예외를 다시 던진다. */
    public void commit() {
        List<ChatRecord> batch = List.copyOf(pending);
        pending.clear();
        try {
            producer.commitTransaction();
        } catch (RuntimeException e) {
            failedToUser.addAll(batch);
            throw e;
        }
        ackedToUser.addAll(batch);
    }

    /** 중단한다. 결과와 관계없이 열린 트랜잭션의 레코드는 실패 응답이다. */
    public void abort() {
        failPending();
        producer.abortTransaction();
    }

    /** producer를 버리기 전에, 열린 트랜잭션에 남은 레코드를 실패 응답으로 정리한다. */
    public void failPending() {
        failedToUser.addAll(pending);
        pending.clear();
    }

    /** 교체 표시 레코드를 트랜잭션 하나로 쓰고 커밋한다. */
    public void writeMarker() throws Exception {
        producer.beginTransaction();
        producer.send(ChatRecord.marker(conversationId, epoch).toProducerRecord(topic)).get();
        producer.commitTransaction();
    }

    /** 열린 트랜잭션의 첫 순번. 열린 트랜잭션이 없으면 다음 순번. */
    public long firstPendingSeq() {
        return pending.isEmpty() ? nextSeq : pending.get(0).seq();
    }

    public List<ChatRecord> brokerAcked() {
        return List.copyOf(brokerAcked);
    }

    public List<ChatRecord> ackedToUser() {
        return List.copyOf(ackedToUser);
    }

    public List<ChatRecord> failedToUser() {
        return List.copyOf(failedToUser);
    }

    public long nextSeq() {
        return nextSeq;
    }

    public long epoch() {
        return epoch;
    }

    public String name() {
        return name;
    }

    /** 클라이언트 TransactionManager의 상태와 마지막 오류. */
    public String clientState() {
        return ShardProducer.transactionState(producer);
    }

    public String producerIdAndEpoch() {
        return ShardProducer.producerIdAndEpoch(producer);
    }

    @Override
    public void close() {
        failPending();
        producer.close(Duration.ofSeconds(5));
    }
}
