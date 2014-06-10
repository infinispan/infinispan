package org.infinispan.security.actions;

import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.interceptors.base.CommandInterceptor;

/**
 * GetCacheInterceptorChainAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheInterceptorChainAction extends AbstractAdvancedCacheAction<List<CommandInterceptor>> {

   public GetCacheInterceptorChainAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public List<CommandInterceptor> run() {
      return cache.getInterceptorChain();
   }

}
