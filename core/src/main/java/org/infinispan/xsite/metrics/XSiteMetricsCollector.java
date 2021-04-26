package org.infinispan.xsite.metrics;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.stat.TimerTracker;

/**
 * Collects metrics about cross-site replication operations.
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
public interface XSiteMetricsCollector {

   Collection<String> sites();

   // -- requests sent statistics -- //

   void recordRequestSent(String dstSite, long duration, TimeUnit timeUnit);

   long getMinRequestSentDuration(String dstSite, long defaultValue, TimeUnit outTimeUnit);

   long getMinRequestSentDuration(long defaultValue, TimeUnit outTimeUnit);

   long getMaxRequestSentDuration(String dstSite, long defaultValue, TimeUnit outTimeUnit);

   long getMaxRequestSentDuration(long defaultValue, TimeUnit outTimeUnit);

   long getAvgRequestSentDuration(String dstSite, long defaultValue, TimeUnit outTimeUnit);

   long getAvgRequestSentDuration(long defaultValue, TimeUnit outTimeUnit);

   long countRequestsSent(String dstSite);

   long countRequestsSent();

   void resetRequestsSent();

   void registerTimer(String dstSite, TimerTracker timer);

   void registerTimer(TimerTracker timer);

   // -- requests received statistics -- //

   void recordRequestsReceived(String srcSite);

   long countRequestsReceived(String srcSite);

   long countRequestsReceived();

   void resetRequestReceived();
}
