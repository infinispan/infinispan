package org.infinispan.security.actions;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * UndefineConfigurationAction.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public final class UndefineConfigurationAction implements Runnable {

   private final EmbeddedCacheManager cacheManager;
   private final String cacheName;

   public UndefineConfigurationAction(EmbeddedCacheManager cacheManager, String cacheName) {
      this.cacheManager = cacheManager;
      this.cacheName = cacheName;
   }

   @Override
   public void run() {
      cacheManager.undefineConfiguration(cacheName);
   }

}
