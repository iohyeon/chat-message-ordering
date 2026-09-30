package lab.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import java.util.function.Predicate;
import org.slf4j.LoggerFactory;

/**
 * 클라이언트 로그를 메모리에 모은다. org.apache.kafka.clients.producer 아래를 DEBUG로 켜되,
 * 표준 출력으로 모두 흘리지 않고 여기에만 모은 뒤 필요한 줄만 보고서에 옮긴다.
 */
final class ClientLogCapture implements AutoCloseable {

    private static final String[] LOGGERS = {
            "org.apache.kafka.clients.producer",
            "org.apache.kafka.clients.consumer.internals.FetchCollector",
            "org.apache.kafka.clients.consumer.internals.CompletedFetch"
    };

    /** 스레드 이름은 로그 이벤트가 만들어진 순간에 고정해야 한다. logback은 처음 읽을 때 채우므로 append에서 읽어 둔다. */
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>() {
        @Override
        protected void append(ILoggingEvent e) {
            e.prepareForDeferredProcessing();
            super.append(e);
        }
    };

    ClientLogCapture() {
        appender.start();
        for (String name : LOGGERS) {
            Logger logger = (Logger) LoggerFactory.getLogger(name);
            logger.setLevel(Level.DEBUG);
            logger.setAdditive(false);
            logger.addAppender(appender);
        }
    }

    void clear() {
        synchronized (appender.list) {
            appender.list.clear();
        }
    }

    List<String> lines(Predicate<String> filter) {
        synchronized (appender.list) {
            return appender.list.stream()
                    .map(e -> e.getLevel() + " [" + e.getThreadName() + "] " + e.getLoggerName()
                            .substring(e.getLoggerName().lastIndexOf('.') + 1) + " - " + e.getFormattedMessage()
                            + (e.getThrowableProxy() != null ? " | " + e.getThrowableProxy().getClassName() + ": "
                            + e.getThrowableProxy().getMessage() : ""))
                    .filter(filter)
                    .toList();
        }
    }

    @Override
    public void close() {
        for (String name : LOGGERS) {
            Logger logger = (Logger) LoggerFactory.getLogger(name);
            logger.detachAppender(appender);
            logger.setLevel(null);
            logger.setAdditive(true);
        }
        appender.stop();
    }
}
