package org.infinispan.security.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.interceptors.AsyncInterceptor;

import java.util.List;

/**
 * GetCacheInterceptorChainAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheInterceptorChainAction extends AbstractAdvancedCacheAction<List<AsyncInterceptor>> {

   public GetCacheInterceptorChainAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public List<AsyncInterceptor> run() {
      return cache.getAsyncInterceptorChain().getInterceptors();
   }

}
