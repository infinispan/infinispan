package org.infinispan.security.actions;

import java.security.PrivilegedAction;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * GetCacheAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheAction implements PrivilegedAction<Cache<?, ?>> {

   private final String cacheName;
   private final EmbeddedCacheManager cacheManager;

   public GetCacheAction(EmbeddedCacheManager cacheManager, String cacheName) {
      this.cacheManager = cacheManager;
      this.cacheName = cacheName;
   }

   @Override
   public Cache<?, ?> run() {
      return cacheManager.getCache(cacheName);
   }

}
