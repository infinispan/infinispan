package org.infinispan.server;

import static org.infinispan.security.Security.doPrivileged;

import java.security.Provider;
import java.util.Collection;
import java.util.function.Supplier;

import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactoryBuilder;
import javax.naming.spi.NamingManager;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.Authorizer;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;

/**
 * SecurityActions for the org.infinispan.server.server package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant <tristan@infinispan.org>
 * @since 10.0
 */
final class SecurityActions {

   static void addSecurityProvider(Provider provider) {
      if (java.security.Security.getProvider(provider.getName()) == null) {
         java.security.Security.insertProviderAt(provider, 1);
      }
   }

   static void startCacheManager(final EmbeddedCacheManager cacheManager) {
      Runnable action = () -> {
         cacheManager.start();
      };
      doPrivileged(action);
   }

   static boolean stopCacheManager(final EmbeddedCacheManager cacheManager) {
      Supplier<Boolean> action = () -> {
         if (cacheManager != null) {
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

   static void postStartProtocolServer(Collection<ProtocolServer> servers) {
      doPrivileged(() -> {
         servers.forEach(ProtocolServer::postStart);
      });
   }

   static GlobalConfiguration getCacheManagerConfiguration(EmbeddedCacheManager manager) {
      return org.infinispan.security.actions.SecurityActions.getCacheManagerConfiguration(manager);
   }

   static void shutdownAllCaches(DefaultCacheManager manager) {
      Runnable action = manager::shutdownAllCaches;
      doPrivileged(action);
   }

   static GlobalComponentRegistry getGlobalComponentRegistry(final EmbeddedCacheManager cacheManager) {
      return org.infinispan.security.actions.SecurityActions.getGlobalComponentRegistry(cacheManager);
   }


   static void setInitialContextFactoryBuilder(InitialContextFactoryBuilder initialContextFactoryBuilder) throws NamingException {
      NamingManager.setInitialContextFactoryBuilder(initialContextFactoryBuilder);
   }

   static void checkPermission(EmbeddedCacheManager cacheManager, AuthorizationPermission permission) {
      Authorizer authorizer = getGlobalComponentRegistry(cacheManager).getComponent(Authorizer.class);
      authorizer.checkPermission(cacheManager.getSubject(), permission);
   }

   static ClusterExecutor getClusterExecutor(EmbeddedCacheManager cacheManager) {
      return doPrivileged(cacheManager::executor);
   }
}
