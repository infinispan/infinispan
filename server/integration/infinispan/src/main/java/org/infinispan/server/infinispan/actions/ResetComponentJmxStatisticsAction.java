package org.infinispan.server.infinispan.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.jmx.JmxStatisticsExposer;

/**
 * ResetComponentJmxStatisticsAction.
 * This class can be used to reset the statistics for a given interceptor that implements
 * {@link org.infinispan.interceptors.base.JmxStatsCommandInterceptor} and it is a component in the cache's registry.
 *
 * @author wburns
 * @since 7.0
 */
public class ResetComponentJmxStatisticsAction<T extends JmxStatisticsExposer> extends AbstractAdvancedCacheAction<Void> {
   private final Class<T> interceptorClass;

   public ResetComponentJmxStatisticsAction(AdvancedCache<?, ?> cache, Class<T> interceptorClass) {
      super(cache);
      this.interceptorClass = interceptorClass;
   }

   @Override
   public Void run() {
      T interceptor = cache.getComponentRegistry().getComponent(interceptorClass);
      if (interceptor != null) {
         interceptor.resetStatistics();
      }
      return null;
   }
}
