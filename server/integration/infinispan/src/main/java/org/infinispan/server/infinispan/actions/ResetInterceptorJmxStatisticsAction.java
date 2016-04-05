package org.infinispan.server.infinispan.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.interceptors.SequentialInterceptor;
import org.infinispan.jmx.JmxStatisticsExposer;

import static org.jboss.as.clustering.infinispan.subsystem.CacheMetricsHandler.getFirstInterceptorWhichExtends;

/**
 * ResetInterceptorJmxStatisticsAction.
 * This class can be used to reset the statistics for a given interceptor that implements
 * {@link org.infinispan.interceptors.impl.JmxStatsCommandInterceptor} and is an interceptor in the cache's chain.
 *
 * @author wburns
 * @since 7.0
 */
public class ResetInterceptorJmxStatisticsAction<T extends SequentialInterceptor & JmxStatisticsExposer> extends AbstractAdvancedCacheAction<Void> {
   private final Class<T> interceptorClass;

   public ResetInterceptorJmxStatisticsAction(AdvancedCache<?, ?> cache, Class<T> interceptorClass) {
      super(cache);
      this.interceptorClass = interceptorClass;
   }

   @Override
   public Void run() {
      T interceptor = getFirstInterceptorWhichExtends(cache.getSequentialInterceptorChain().getInterceptors(), interceptorClass);
      if (interceptor != null) {
         interceptor.resetStatistics();
      }
      return null;
   }
}
