package org.infinispan.security.actions;

import java.util.function.Supplier;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * GetOrCreateCacheAction.
 *
 * @author Tristan Tarrant
 * @since 12.1
 */
public class GetOrCreateTemplateAction implements Supplier<Configuration> {

   private final String cacheName;
   private final EmbeddedCacheManager cacheManager;
   private final Configuration configuration;

   public GetOrCreateTemplateAction(EmbeddedCacheManager cacheManager, String cacheName, Configuration configuration) {
      this.cacheManager = cacheManager;
      this.cacheName = cacheName;
      this.configuration = configuration;
   }

   @Override
   public Configuration get() {
      return cacheManager.administration().getOrCreateTemplate(cacheName, configuration);
   }
}
