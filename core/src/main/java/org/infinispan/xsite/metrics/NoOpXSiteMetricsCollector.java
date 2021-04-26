package org.infinispan.xsite.metrics;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.stat.TimerTracker;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * An no-op implementation for {@link XSiteMetricsCollector}.
 * <p>
 * Used when cross-site replication isn't enabled on a cache.
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
@Scope(Scopes.NAMED_CACHE)
public class NoOpXSiteMetricsCollector implements XSiteMetricsCollector {

   private static final NoOpXSiteMetricsCollector INSTANCE = new NoOpXSiteMetricsCollector();

   private NoOpXSiteMetricsCollector() {
   }

   public static NoOpXSiteMetricsCollector getInstance() {
      return INSTANCE;
   }

   @Override
   public Collection<String> sites() {
      return Collections.emptyList();
   }

   @Override
   public void recordRequestSent(String dstSite, long duration, TimeUnit timeUnit) {
      //no-op
   }

   @Override
   public long getMinRequestSentDuration(String dstSite, long defaultValue, TimeUnit outTimeUnit) {
      return defaultValue;
   }

   @Override
   public long getMinRequestSentDuration(long defaultValue, TimeUnit outTimeUnit) {
      return defaultValue;
   }

   @Override
   public long getMaxRequestSentDuration(String dstSite, long defaultValue, TimeUnit outTimeUnit) {
      return defaultValue;
   }

   @Override
   public long getMaxRequestSentDuration(long defaultValue, TimeUnit outTimeUnit) {
      return defaultValue;
   }

   @Override
   public long getAvgRequestSentDuration(String dstSite, long defaultValue, TimeUnit outTimeUnit) {
      return defaultValue;
   }

   @Override
   public long getAvgRequestSentDuration(long defaultValue, TimeUnit outTimeUnit) {
      return defaultValue;
   }

   @Override
   public long countRequestsSent(String dstSite) {
      return 0;
   }

   @Override
   public long countRequestsSent() {
      return 0;
   }

   @Override
   public void resetRequestsSent() {
      //no-op
   }

   @Override
   public void registerTimer(String dstSite, TimerTracker timer) {
      //no-op
   }

   @Override
   public void registerTimer(TimerTracker timer) {
      //no-op
   }

   @Override
   public void recordRequestsReceived(String srcSite) {
      //no-op;
   }

   @Override
   public long countRequestsReceived(String srcSite) {
      return 0;
   }

   @Override
   public long countRequestsReceived() {
      return 0;
   }

   @Override
   public void resetRequestReceived() {
      //no-op
   }
}
