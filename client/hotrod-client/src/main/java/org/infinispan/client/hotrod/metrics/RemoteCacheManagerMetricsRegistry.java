package org.infinispan.client.hotrod.metrics;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.commons.stat.CounterTracker;
import org.infinispan.commons.stat.DistributionSummaryTracker;
import org.infinispan.commons.stat.TimerTracker;

/**
 * Hot Rod client entrypoint.
 * <p>
 * All metrics registered in this instance are considered global to the client. Per cache metrics are registered using
 * {@link #withCache(String)}, where the cache's name is used to distinct the metrics. The implementation is responsible
 * to separate each cache metric, because all caches register the same set of metrics (reads, writes, etc.).
 *
 * @since 15.1
 */
public interface RemoteCacheManagerMetricsRegistry extends HotRodClientMetricsRegistry {

   RemoteCacheManagerMetricsRegistry DISABLED = new RemoteCacheManagerMetricsRegistry() {
      @Override
      public HotRodClientMetricsRegistry withCache(String cacheName) {
         return HotRodClientMetricsRegistry.DISABLED;
      }

      @Override
      public void removeCache(String cacheName) {
         //no-op
      }

      @Override
      public void createGauge(String metricName, String description, Supplier<Number> gauge, Map<String, String> tags, Consumer<Object> generatedId) {
         //no-op
      }

      @Override
      public void createTimeGauge(String metricName, String description, Supplier<Number> gauge, TimeUnit timeUnit, Map<String, String> tags, Consumer<Object> generatedId) {
         //no-op
      }

      @Override
      public TimerTracker createTimer(String metricName, String description, Map<String, String> tags, Consumer<Object> generatedId) {
         return TimerTracker.NO_OP;
      }

      @Override
      public CounterTracker createCounter(String metricName, String description, Map<String, String> tags, Consumer<Object> generatedId) {
         return CounterTracker.NO_OP;
      }

      @Override
      public DistributionSummaryTracker createDistributionSummery(String metricName, String description, Map<String, String> tags, Consumer<Object> generatedId) {
         return DistributionSummaryTracker.NO_OP;
      }

      @Override
      public void close() {
         //no-op
      }

      @Override
      public void removeMetric(Object id) {
         //no-op
      }
   };

   /**
    * Returns a {@link HotRodClientMetricsRegistry} to be used to register a remote cache metrics.
    *
    * @param cacheName The cache's name.
    * @return The {@link HotRodClientMetricsRegistry} implementation to use.
    */
   HotRodClientMetricsRegistry withCache(String cacheName);

   /**
    * Removes and unregister all cache related metrics.
    *
    * @param cacheName The cache's name.
    */
   void removeCache(String cacheName);

}
