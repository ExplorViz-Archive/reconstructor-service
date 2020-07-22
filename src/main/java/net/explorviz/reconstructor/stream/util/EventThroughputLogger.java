package net.explorviz.reconstructor.stream.util;

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;

/**
 * Helper class to log the throughput of events, i.e. duration/events. Decorates a {@link Logger} to
 * emit logs.
 */
public class EventThroughputLogger {


  private final Logger logger;

  private int counter = 0;
  private final int threshold;

  private Instant start;


  /**
   * Creates a new instance of a logger that logs the duration it took to process a given amount of
   * records.
   *
   * @param logger the logger to use
   * @param threshold the events to log until an output is made
   */
  public EventThroughputLogger(final Logger logger, final int threshold) {
    this.logger = logger;
    this.threshold = threshold;
  }

  /**
   * Same as {@link #EventThroughputLogger(Logger, int)} with threshold of 1000.
   */
  public EventThroughputLogger(final Logger logger) {
    this(logger, 1000);
  }

  /**
   * Log that an event occurred. If the amount of logged events is equal to the threshold, a log
   * will be emitted with the duration it took to log the amount of events
   */
  public void logEvent() {
    if (this.counter == 0) {
      this.start = Instant.now();
    }
    this.counter++;
    if (this.counter % this.threshold == 0) {
      this.emitLog();
      this.counter = 0;
    }
  }

  private void emitLog() {
    final Duration duration = Duration.between(this.start, Instant.now());
    final String df = new DecimalFormat("####").format(duration.toMillis());
    if (this.logger.isInfoEnabled()) {
      this.logger.info("Took {} to log {} events", df, this.threshold);
    }

  }


}
