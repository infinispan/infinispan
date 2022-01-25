package org.infinispan.xsite.metrics;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.infinispan.commons.stat.SimpleStat;
import org.infinispan.commons.stat.SimpleStateWithTimer;
import org.infinispan.commons.stat.TimerTracker;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Implementation of {@link XSiteMetricsCollector} to use when asynchronous backups (cross-site replication) are
 * configured.
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
@Scope(Scopes.NAMED_CACHE)
public class DefaultXSiteMetricsCollector implements XSiteMetricsCollector {

   private static final long NON_EXISTING = -1;
   private final Map<String, SiteMetric> siteMetricMap;
   private final SimpleStat globalRequestsSent;

   public DefaultXSiteMetricsCollector(Configuration configuration) {
      siteMetricMap = new ConcurrentHashMap<>();
      configuration.sites().allBackupsStream().forEach(c -> siteMetricMap.put(c.site(), new SiteMetric()));
      globalRequestsSent = new SimpleStateWithTimer();
   }

   private static SiteMetric newSiteMetric(String site) {
      return new SiteMetric();
   }

   private static long convert(long value, long defaultValue, TimeUnit timeUnit) {
      return value == NON_EXISTING ? defaultValue : timeUnit.convert(value, TimeUnit.NANOSECONDS);
   }

   @Override
   public Collection<String> sites() {
      return Collections.unmodifiableCollection(siteMetricMap.keySet());
   }

   @Override
   public void recordRequestSent(String dstSite, long duration, TimeUnit timeUnit) {
      assert duration > 0;
      long durationNanos = timeUnit.toNanos(duration);
      getSiteMetricToRecord(dstSite).getRequestsSent().record(durationNanos);
      globalRequestsSent.record(durationNanos);
   }

   @Override
   public long getMinRequestSentDuration(String dstSite, long defaultValue, TimeUnit outTimeUnit) {
      SiteMetric metric = siteMetricMap.get(dstSite);
      if (metric == null) {
         return defaultValue;
      }
      long val = metric.getRequestsSent().getMin(NON_EXISTING);
      return convert(val, defaultValue, outTimeUnit);
   }

   @Override
   public long getMinRequestSentDuration(long defaultValue, TimeUnit outTimeUnit) {
      long val = globalRequestsSent.getMin(NON_EXISTING);
      return convert(val, defaultValue, outTimeUnit);
   }

   @Override
   public long getMaxRequestSentDuration(String dstSite, long defaultValue, TimeUnit outTimeUnit) {
      SiteMetric metric = siteMetricMap.get(dstSite);
      if (metric == null) {
         return defaultValue;
      }
      long val = metric.getRequestsSent().getMax(NON_EXISTING);
      return convert(val, defaultValue, outTimeUnit);
   }

   @Override
   public long getMaxRequestSentDuration(long defaultValue, TimeUnit outTimeUnit) {
      long val = globalRequestsSent.getMax(NON_EXISTING);
      return convert(val, defaultValue, outTimeUnit);
   }

   @Override
   public long getAvgRequestSentDuration(String dstSite, long defaultValue, TimeUnit outTimeUnit) {
      SiteMetric metric = siteMetricMap.get(dstSite);
      if (metric == null) {
         return defaultValue;
      }
      long val = metric.getRequestsSent().getAverage(NON_EXISTING);
      return convert(val, defaultValue, outTimeUnit);
   }

   @Override
   public long getAvgRequestSentDuration(long defaultValue, TimeUnit outTimeUnit) {
      long val = globalRequestsSent.getAverage(NON_EXISTING);
      return convert(val, defaultValue, outTimeUnit);
   }

   @Override
   public long countRequestsSent(String dstSite) {
      SiteMetric metric = siteMetricMap.get(dstSite);
      return metric == null ? 0 : metric.getRequestsSent().count();
   }

   @Override
   public long countRequestsSent() {
      return globalRequestsSent.count();
   }

   @Override
   public void resetRequestsSent() {
      globalRequestsSent.reset();
      siteMetricMap.values().forEach(siteMetric -> siteMetric.getRequestsSent().reset());
   }

   @Override
   public void registerTimer(String dstSite, TimerTracker timer) {
      getSiteMetricToRecord(dstSite).getRequestsSent().setTimer(timer);
   }

   @Override
   public void registerTimer(TimerTracker timer) {
      globalRequestsSent.setTimer(timer);
   }

   @Override
   public void recordRequestsReceived(String srcSite) {
      getSiteMetricToRecord(srcSite).getRequestsReceived().increment();
   }

   @Override
   public long countRequestsReceived(String srcSite) {
      SiteMetric metric = siteMetricMap.get(srcSite);
      return metric == null ? 0 : metric.getRequestsReceived().sum();
   }

   @Override
   public long countRequestsReceived() {
      return siteMetricMap.values().stream()
                          .map(SiteMetric::getRequestsReceived)
                          .map(LongAdder::sum)
                          .reduce(0L, Long::sum);
   }

   @Override
   public void resetRequestReceived() {
      siteMetricMap.values().forEach(siteMetric -> siteMetric.getRequestsReceived().reset());
   }

   private SiteMetric getSiteMetricToRecord(String dstSite) {
      return siteMetricMap.computeIfAbsent(dstSite, DefaultXSiteMetricsCollector::newSiteMetric);
   }

   /**
    * Per site statistics (requests sent/received and response times)
    */
   private static class SiteMetric {
      private final SimpleStat requestsSent = new SimpleStateWithTimer();
      private final LongAdder requestsReceived = new LongAdder();

      public SimpleStat getRequestsSent() {
         return requestsSent;
      }

      public LongAdder getRequestsReceived() {
         return requestsReceived;
      }
   }

}
