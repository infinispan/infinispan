package org.infinispan.server;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.Provider;
import java.util.Properties;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheManagerConfigurationAction;
import org.infinispan.security.actions.GetGlobalComponentRegistryAction;
import org.infinispan.server.core.CacheIgnoreManager;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;

/**
 * SecurityActions for the org.infinispan.server.server package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other {@link
 * java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant <tristan@infinispan.org>
 * @since 10.0
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static Properties getSystemProperties() {
      return doPrivileged(() -> System.getProperties());
   }

   static void addSecurityProvider(Provider provider) {
      doPrivileged(() -> {
               if (java.security.Security.getProvider(provider.getName()) == null) {
                  java.security.Security.insertProviderAt(provider, 1);
               }
               return null;
            }
      );
   }

   static void startCacheManager(final EmbeddedCacheManager cacheManager) {
      PrivilegedAction<Void> action = () -> {
         cacheManager.start();
         return null;
      };
      doPrivileged(action);
   }

   static boolean stopCacheManager(final EmbeddedCacheManager cacheManager) {
      PrivilegedAction<Boolean> action = () -> {
         if (cacheManager.getStatus().allowInvocations()) {
            cacheManager.stop();
            return true;
         } else {
            return false;
         }
      };
      return doPrivileged(action);
   }

   static void startProtocolServer(final ProtocolServer server, final ProtocolServerConfiguration configuration, final EmbeddedCacheManager cacheManager, final CacheIgnoreManager cacheIgnoreManager) {
      PrivilegedAction<Void> action = () -> {
         server.start(configuration, cacheManager, cacheIgnoreManager);
         return null;
      };
      doPrivileged(action);
   }

   static GlobalConfiguration getCacheManagerConfiguration(EmbeddedCacheManager manager) {
      return doPrivileged(new GetCacheManagerConfigurationAction(manager));
   }

   static void shutdownCache(EmbeddedCacheManager manager, String name) {
      PrivilegedAction<Void> action = () -> {
         manager.getCache(name).shutdown();
         return null;
      };
      doPrivileged(action);
   }

   static GlobalComponentRegistry getGlobalComponentRegistry(final EmbeddedCacheManager cacheManager) {
      GetGlobalComponentRegistryAction action = new GetGlobalComponentRegistryAction(cacheManager);
      return doPrivileged(action);
   }
}
