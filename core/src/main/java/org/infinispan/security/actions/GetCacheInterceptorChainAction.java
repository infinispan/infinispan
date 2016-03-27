package org.infinispan.security.actions;

import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.interceptors.SequentialInterceptor;

/**
 * GetCacheInterceptorChainAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheInterceptorChainAction extends AbstractAdvancedCacheAction<List<SequentialInterceptor>> {

   public GetCacheInterceptorChainAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public List<SequentialInterceptor> run() {
      return cache.getSequentialInterceptorChain().getInterceptors();
   }

}
