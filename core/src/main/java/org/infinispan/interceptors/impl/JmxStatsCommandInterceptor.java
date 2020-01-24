package org.infinispan.interceptors.impl;

import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;

/**
 * Base class for all the interceptors exposing management statistics.
 *
 * @author Mircea.Markus@jboss.com
 * @since 9.0
 */
@MBean
public abstract class JmxStatsCommandInterceptor extends DDAsyncInterceptor implements JmxStatisticsExposer {

   private boolean statisticsEnabled = false;

   @Start
   public final void onStart() {
      setStatisticsEnabled(cacheConfiguration.statistics().enabled());
   }

   /**
    * Returns whether an interceptor's statistics are being captured.
    *
    * @return true if statistics are captured
    */
   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", writable = true)
   @Override
   public boolean getStatisticsEnabled() {
      return statisticsEnabled;
   }

   /**
    * @param enabled whether gathering statistics for JMX are enabled.
    */
   @Override
   public void setStatisticsEnabled(boolean enabled) {
      statisticsEnabled = enabled;
   }

   /**
    * Resets statistics gathered.  Is a no-op, and should be overridden if it is to be meaningful.
    */
   @ManagedOperation(displayName = "Reset Statistics", description = "Resets statistics gathered by this component")
   @Override
   public void resetStatistics() {
   }
}
