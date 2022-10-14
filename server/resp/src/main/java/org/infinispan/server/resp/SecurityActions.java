package org.infinispan.server.resp;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetOrCreateCacheWithTemplateNameAction;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * SecurityActions for the org.infinispan.server.resp package. Do not move and do not change class and method
 * visibility!
 *
 * @since 14
 */
final class SecurityActions {

   private SecurityActions() {
   }

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      return System.getSecurityManager() != null ?
            AccessController.doPrivileged(action) : Security.doPrivileged(action);
   }

   static Cache<?, ?> getOrCreateCache(EmbeddedCacheManager cm, String cacheName, String templateName) {
      GetOrCreateCacheWithTemplateNameAction action = new GetOrCreateCacheWithTemplateNameAction(cm, cacheName, templateName);
      return doPrivileged(action);
   }
}
