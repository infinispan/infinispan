package org.infinispan.server.tasks.admin;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetGlobalComponentRegistryAction;
import org.infinispan.security.impl.AuthorizationHelper;

/**
 * SecurityActions for the org.infinispan.server.tasks.admin package. Do not move and do not change class and method
 * visibility!
 *
 * @author Tristan Tarrant
 * @since 12.0
 */
final class SecurityActions {

   private SecurityActions() {
   }

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      return System.getSecurityManager() != null ?
            AccessController.doPrivileged(action) : Security.doPrivileged(action);
   }

   static GlobalComponentRegistry getGlobalComponentRegistry(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetGlobalComponentRegistryAction(cacheManager));
   }

   static void checkPermission(EmbeddedCacheManager cacheManager, AuthorizationPermission permission) {
      AuthorizationHelper authzHelper = getGlobalComponentRegistry(cacheManager).getComponent(AuthorizationHelper.class);
      authzHelper.checkPermission(Security.getSubject(), permission);
   }
}
