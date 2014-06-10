package org.infinispan.security.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.security.AuthorizationManager;

/**
 * GetCacheAuthorizationManagerAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheAuthorizationManagerAction extends AbstractAdvancedCacheAction<AuthorizationManager> {
   public GetCacheAuthorizationManagerAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public AuthorizationManager run() {
      return cache.getAuthorizationManager();
   }

}
