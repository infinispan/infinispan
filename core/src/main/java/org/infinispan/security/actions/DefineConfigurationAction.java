package org.infinispan.security.actions;

import java.security.PrivilegedAction;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * DefineConfigurationAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public final class DefineConfigurationAction implements PrivilegedAction<Void> {

   private final EmbeddedCacheManager cacheManager;
   private final String cacheName;
   private final Configuration configurationOverride;

   public DefineConfigurationAction(EmbeddedCacheManager cacheManager, String cacheName, Configuration configurationOverride) {
      this.cacheManager = cacheManager;
      this.cacheName = cacheName;
      this.configurationOverride = configurationOverride;
   }

   @Override
   public Void run() {
      cacheManager.defineConfiguration(cacheName, configurationOverride);
      return null;
   }

}
