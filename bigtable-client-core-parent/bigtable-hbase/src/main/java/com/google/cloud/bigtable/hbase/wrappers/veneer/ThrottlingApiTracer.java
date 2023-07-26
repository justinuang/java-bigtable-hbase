package com.google.cloud.bigtable.hbase.wrappers.veneer;

import com.google.api.gax.tracing.BaseApiTracer;
import com.google.cloud.bigtable.hbase.util.Logger;
import java.util.concurrent.atomic.AtomicLong;
import org.threeten.bp.Duration;

public class ThrottlingApiTracer extends BaseApiTracer {

  protected static final Logger LOG = new Logger(ThrottlingApiTracer.class);

  long attemptStartMillis = 0;

  public static AtomicLong throttling_ms = new AtomicLong(0);

  @Override
  public void operationSucceeded() {
  }

  @Override
  public void operationCancelled() {

  }

  @Override
  public void operationFailed(Throwable throwable) {

  }

  @Override
  public void connectionSelected(String s) {

  }

  @Override
  public void attemptStarted(int i) {
    attemptStartMillis = System.currentTimeMillis();
    LOG.info("Attempt start %s", attemptStartMillis);
  }

  @Override
  public void attemptStarted(Object o, int i) {
    attemptStartMillis = System.currentTimeMillis();
    LOG.info("Attempt start i %s", attemptStartMillis);
  }

  @Override
  public void attemptSucceeded() {
    long duration_millis = System.currentTimeMillis() - attemptStartMillis;
    LOG.info("Attempt succeeded %s", duration_millis);
  }

  @Override
  public void attemptCancelled() {

  }

  @Override
  public void attemptFailed(Throwable throwable, Duration duration) {
    long duration_millis = System.currentTimeMillis() - attemptStartMillis;
    LOG.info("Attempt failed with latency %s, with delay duration: %s", duration_millis, duration.toMillis());
    throttling_ms.addAndGet(duration.toMillis());
    throttling_ms.addAndGet(duration_millis);
    // counter.inc(duration.toMillis());
    // counter.inc(duration_millis);
  }

  @Override
  public void attemptFailedRetriesExhausted(Throwable throwable) {

  }

  @Override
  public void attemptPermanentFailure(Throwable throwable) {

  }

  @Override
  public void lroStartFailed(Throwable throwable) {

  }

  @Override
  public void lroStartSucceeded() {

  }

  @Override
  public void responseReceived() {

  }

  @Override
  public void requestSent() {

  }

  @Override
  public void batchRequestSent(long l, long l1) {

  }
}
