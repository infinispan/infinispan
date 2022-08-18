package org.infinispan.server;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.Provider;
import java.util.Properties;

import javax.naming.spi.InitialContextFactoryBuilder;
import javax.naming.spi.NamingManager;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheManagerConfigurationAction;
import org.infinispan.security.actions.GetGlobalComponentRegistryAction;
import org.infinispan.security.impl.Authorizer;
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

   private static <T> T doPrivilegedExceptionAction(PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static Properties getSystemProperties() {
      return doPrivileged(System::getProperties);
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
         if (cacheManager != null && cacheManager.getStatus().allowInvocations()) {
            cacheManager.stop();
            return true;
         } else {
            return false;
         }
      };
      return doPrivileged(action);
   }

   static void startProtocolServer(ProtocolServer<ProtocolServerConfiguration> server, ProtocolServerConfiguration configuration, EmbeddedCacheManager cacheManager) {
      doPrivileged(() -> {
         server.start(configuration, cacheManager);
         return null;
      });
   }

   static GlobalConfiguration getCacheManagerConfiguration(EmbeddedCacheManager manager) {
      return doPrivileged(new GetCacheManagerConfigurationAction(manager));
   }

   static void shutdownAllCaches(DefaultCacheManager manager) {
      PrivilegedAction<Void> action = () -> {
         manager.shutdownAllCaches();
         return null;
      };
      doPrivileged(action);
   }

   static GlobalComponentRegistry getGlobalComponentRegistry(final EmbeddedCacheManager cacheManager) {
      GetGlobalComponentRegistryAction action = new GetGlobalComponentRegistryAction(cacheManager);
      return doPrivileged(action);
   }


   static void setInitialContextFactoryBuilder(InitialContextFactoryBuilder initialContextFactoryBuilder) throws PrivilegedActionException {
      doPrivilegedExceptionAction(() -> {
         NamingManager.setInitialContextFactoryBuilder(initialContextFactoryBuilder);
         return null;
      });
   }

   static void checkPermission(EmbeddedCacheManager cacheManager, AuthorizationPermission permission) {
      Authorizer authorizer = getGlobalComponentRegistry(cacheManager).getComponent(Authorizer.class);
      authorizer.checkPermission(cacheManager.getSubject(), permission);
   }

   static ClusterExecutor getClusterExecutor(EmbeddedCacheManager cacheManager) {
      return doPrivileged(() -> cacheManager.executor());
   }
}
