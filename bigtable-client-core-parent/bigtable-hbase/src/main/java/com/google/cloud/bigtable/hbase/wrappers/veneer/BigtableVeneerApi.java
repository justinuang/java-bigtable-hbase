/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.hbase.wrappers.veneer;

import com.google.api.core.InternalApi;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.tracing.ApiTracer;
import com.google.api.gax.tracing.ApiTracerFactory;
import com.google.api.gax.tracing.BaseApiTracer;
import com.google.api.gax.tracing.SpanName;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import com.google.cloud.bigtable.hbase.BigtableBufferedMutatorHelper;
import com.google.cloud.bigtable.hbase.util.Logger;
import com.google.cloud.bigtable.hbase.wrappers.AdminClientWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BigtableApi;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.cloud.bigtable.metrics.Counter;
import java.io.IOException;
import org.apache.beam.sdk.metrics.DelegatingCounter;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.Metrics;
import org.threeten.bp.Duration;

/**
 * For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableVeneerApi extends BigtableApi {

  protected static final Logger LOG = new Logger(BigtableVeneerApi.class);

  private final Counter activeSessions =
      BigtableClientMetrics.counter(MetricLevel.Info, "session.active");

  private static final SharedDataClientWrapperFactory sharedClientFactory =
      new SharedDataClientWrapperFactory();
  private final DataClientWrapper dataClientWrapper;
  private final AdminClientWrapper adminClientWrapper;
  private final int channelPoolSize;

  org.apache.beam.sdk.metrics.Counter counter = Metrics.counter("dataflow-throttling-metrics", "throttling-msecs");

  public BigtableVeneerApi(BigtableHBaseVeneerSettings settings) throws IOException {
    super(settings);

    // active channel count is hard coded at client creation time based on the setting. If
    // transportChannelProvider in the data setting is not InstantiatingGrpcChannelProvider, this
    // count wil not be present. If channel pool caching is enabled, channel pool size is calculated
    // in SharedDataClientWrapperFactory to avoid incrementing/decrementing the same channel
    // multiple times for the same key.

    ApiTracerFactory tracerFactory = new ApiTracerFactory() {
      @Override
      public ApiTracer newTracer(ApiTracer apiTracer, SpanName spanName,
          OperationType operationType) {
        return new BaseApiTracer() {
          long attemptStartMillis = 0;

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
        };
      }
    };

    BigtableDataSettings.Builder builder = settings.getDataSettings().toBuilder();
    builder.stubSettings().setTracerFactory(tracerFactory);
    builder.stubSettings().readRowsSettings().retrySettings().setInitialRetryDelay(Duration.ofSeconds(1)).setRetryDelayMultiplier(1).setMaxAttempts(1000000).setMaxRetryDelay(Duration.ofMinutes(10));
    builder.stubSettings().bulkReadRowsSettings().retrySettings().setInitialRetryDelay(Duration.ofSeconds(1)).setRetryDelayMultiplier(1).setMaxAttempts(1000000).setMaxRetryDelay(Duration.ofMinutes(10));
    dataClientWrapper =
        new DataClientVeneerApi(
            BigtableDataClient.create(builder.build()), settings.getClientTimeouts());
    channelPoolSize = getChannelPoolSize(settings.getDataSettings().getStubSettings());
    for (int i = 0; i < channelPoolSize; i++) {
      BigtableClientMetrics.counter(MetricLevel.Info, "grpc.channel.active").inc();
    }

    BigtableInstanceAdminSettings instanceAdminSettings = settings.getInstanceAdminSettings();
    adminClientWrapper =
        new AdminClientVeneerApi(
            BigtableTableAdminClient.create(settings.getTableAdminSettings()),
            BigtableInstanceAdminClient.create(instanceAdminSettings));
    activeSessions.inc();
  }

  @Override
  public AdminClientWrapper getAdminClient() {
    return adminClientWrapper;
  }

  @Override
  public DataClientWrapper getDataClient() {
    return dataClientWrapper;
  }

  @Override
  public void close() throws IOException {
    dataClientWrapper.close();
    adminClientWrapper.close();
    activeSessions.dec();
    for (int i = 0; i < channelPoolSize; i++) {
      BigtableClientMetrics.counter(MetricLevel.Info, "grpc.channel.active").dec();
    }
  }

  static int getChannelPoolSize(EnhancedBigtableStubSettings stubSettings) {
    if (stubSettings.getTransportChannelProvider() instanceof InstantiatingGrpcChannelProvider) {
      InstantiatingGrpcChannelProvider channelProvider =
          (InstantiatingGrpcChannelProvider) stubSettings.getTransportChannelProvider();
      return channelProvider.toBuilder().getPoolSize();
    }
    return 0;
  }
}
