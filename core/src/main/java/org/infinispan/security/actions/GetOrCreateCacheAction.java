package org.infinispan.security.actions;

import java.security.PrivilegedAction;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * GetOrCreateCacheAction.
 *
 * @author Tristan Tarrant
 * @since 12.1
 */
public class GetOrCreateCacheAction implements PrivilegedAction<Cache<?, ?>> {

   private final String cacheName;
   private final EmbeddedCacheManager cacheManager;
   private final Configuration configuration;

   public GetOrCreateCacheAction(EmbeddedCacheManager cacheManager, String cacheName, Configuration configuration) {
      this.cacheManager = cacheManager;
      this.cacheName = cacheName;
      this.configuration = configuration;
   }

   @Override
   public Cache<?, ?> run() {
      return cacheManager.administration().getOrCreateCache(cacheName, configuration);
   }
}
