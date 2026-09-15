package org.infinispan.core.test.jupiter;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;

/**
 * SecurityActions for the test harness module.
 *
 * @since 16.2
 */
final class SecurityActions {

   private SecurityActions() {
   }

   static GlobalComponentRegistry getGlobalComponentRegistry(EmbeddedCacheManager cacheManager) {
      return Security.doPrivileged(() -> GlobalComponentRegistry.of(cacheManager));
   }
}
