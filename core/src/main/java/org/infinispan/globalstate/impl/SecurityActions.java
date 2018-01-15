package org.infinispan.globalstate.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.DefineConfigurationAction;

/**
 * SecurityActions for the org.infinispan.globalstate.impl package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static void defineConfiguration(final EmbeddedCacheManager cacheManager, final String cacheName, final Configuration configurationOverride) {
      DefineConfigurationAction action = new DefineConfigurationAction(cacheManager, cacheName, configurationOverride);
      if (System.getSecurityManager() != null) {
         AccessController.doPrivileged(action);
      } else {
         Security.doPrivileged(action);
      }
   }
}
