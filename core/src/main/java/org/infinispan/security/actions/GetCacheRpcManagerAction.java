package org.infinispan.security.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.remoting.rpc.RpcManager;

/**
 * GetCacheRpcManagerAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheRpcManagerAction extends AbstractAdvancedCacheAction<RpcManager> {

   public GetCacheRpcManagerAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public RpcManager run() {
      return cache.getRpcManager();
   }

}
