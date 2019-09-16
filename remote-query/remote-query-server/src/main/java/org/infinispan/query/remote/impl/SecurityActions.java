package org.infinispan.query.remote.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.AddCacheDependencyAction;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;
import org.infinispan.security.impl.SecureCacheImpl;

/**
 * SecurityActions for the org.infinispan.query.remote.impl package. Do not move and do not change class and method
 * visibility!
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
final class SecurityActions {

   private SecurityActions() {
   }

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      return System.getSecurityManager() != null ?
            AccessController.doPrivileged(action) : Security.doPrivileged(action);
   }

   static RemoteQueryManager getRemoteQueryManager(AdvancedCache<?, ?> cache) {
      return doPrivileged(new GetCacheComponentRegistryAction(cache)).getComponent(RemoteQueryManager.class);
   }

   static AuthorizationManager getCacheAuthorizationManager(AdvancedCache<?, ?> cache) {
      return doPrivileged(cache::getAuthorizationManager);
   }

   static <K, V> Cache<K, V> getUnwrappedCache(EmbeddedCacheManager cacheManager, String cacheName) {
      return doPrivileged(() -> {
         Cache<K, V> cache = cacheManager.getCache(cacheName);
         if (cache instanceof SecureCacheImpl) {
            return ((SecureCacheImpl) cache).getDelegate();
         } else {
            return cache;
         }
      });
   }

   static SerializationContext getSerializationContext(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetSerializationContextAction(cacheManager));
   }

   static void addCacheDependency(EmbeddedCacheManager cacheManager, String from, String to) {
      doPrivileged(new AddCacheDependencyAction(cacheManager, from, to));
   }
}
