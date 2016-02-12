package org.infinispan.interceptors.base;

import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;

/**
 * Base class for all the interceptors exposing management statistics.
 *
 * @author Mircea.Markus@jboss.com
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
public abstract class JmxStatsCommandInterceptor extends CommandInterceptor implements JmxStatisticsExposer {
   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", writable = true)
   private boolean statisticsEnabled = false;

   @Start
   public void checkStatisticsUsed() {
      setStatisticsEnabled(cacheConfiguration.jmxStatistics().enabled());
   }

   /**
    * Returns whether an interceptor's statistics are being captured.
    *
    * @return true if statistics are captured
    */
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
   @Override
   @ManagedOperation(description = "Resets statistics gathered by this component")
   public void resetStatistics() {
   }
}
