package org.infinispan.server.infinispan.task;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;
import org.infinispan.security.actions.GetCacheGlobalComponentRegistryAction;
import org.infinispan.security.actions.GetGlobalComponentRegistryAction;

/**
 * SecurityActions for the org.infinispan.server.infinispan package
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public final class SecurityActions {
    private static <T> T doPrivileged(PrivilegedAction<T> action) {
        if (System.getSecurityManager() != null) {
            return AccessController.doPrivileged(action);
        } else {
            return Security.doPrivileged(action);
        }
    }

   static ComponentRegistry getComponentRegistry(final AdvancedCache<?, ?> cache) {
        GetCacheComponentRegistryAction action = new GetCacheComponentRegistryAction(cache);
        return doPrivileged(action);
    }

    static GlobalComponentRegistry getGlobalComponentRegistry(final EmbeddedCacheManager cacheManager) {
        GetGlobalComponentRegistryAction action = new GetGlobalComponentRegistryAction(cacheManager);
        return doPrivileged(action);
    }

   public static GlobalComponentRegistry getGlobalComponentRegistry(Cache<Object, Object> cache) {
      return doPrivileged(new GetCacheGlobalComponentRegistryAction(cache.getAdvancedCache()));
   }
}
