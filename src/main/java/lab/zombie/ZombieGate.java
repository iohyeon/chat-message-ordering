package lab.zombie;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 옛 담당자를 정해진 지점에서 멈췄다가 깨우는 문.
 *
 * <p>GC 멈춤은 재현이 불안정하므로, 옛 담당자의 애플리케이션 스레드가 {@link #pauseHere()} 에서 멈추고
 * 조정하는 쪽이 {@link #resume()} 을 부를 때까지 기다리게 한다. 주의할 점은 이것이 애플리케이션 스레드만
 * 멈춘다는 것이다. 실제 GC 멈춤은 producer 내부의 Sender 스레드까지 멈추지만, 이 문은 Sender 스레드를
 * 멈추지 않는다. 그래서 멈추기 전에 {@code flush()} 로 보낼 것을 모두 보낸 상태에서 멈추게 한다.
 */
public final class ZombieGate {

    private final CountDownLatch paused = new CountDownLatch(1);
    private final CountDownLatch resumed = new CountDownLatch(1);
    private final Duration maxPause;

    public ZombieGate(Duration maxPause) {
        this.maxPause = maxPause;
    }

    /** 옛 담당자 쪽에서 부른다. 멈췄다고 알리고 깨울 때까지 기다린다. */
    public void pauseHere() {
        paused.countDown();
        try {
            if (!resumed.await(maxPause.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("resume() 이 " + maxPause + " 안에 호출되지 않았다");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** 조정하는 쪽에서 부른다. 옛 담당자가 멈춤 지점에 도착할 때까지 기다린다. */
    public void awaitPaused() {
        try {
            if (!paused.await(maxPause.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("옛 담당자가 " + maxPause + " 안에 멈춤 지점에 오지 않았다");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    public void resume() {
        resumed.countDown();
    }
}
