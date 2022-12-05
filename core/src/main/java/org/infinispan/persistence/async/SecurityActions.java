package org.infinispan.persistence.async;

import static org.infinispan.security.Security.doPrivileged;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.actions.GetGlobalComponentRegistryAction;

/**
 * SecurityActions for the org.infinispan.persistence.async package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author William Burns
 * @since 15.0
 */
final class SecurityActions {

   private SecurityActions() {
   }

   static GlobalComponentRegistry getGlobalComponentRegistry(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetGlobalComponentRegistryAction(cacheManager));
   }
}
