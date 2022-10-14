package org.infinispan.security.actions;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;

import java.security.PrivilegedAction;

/**
 * GetOrCreateCacheWithTemplateNameAction.
 *
 * @since 14
 */
public class GetOrCreateCacheWithTemplateNameAction implements PrivilegedAction<Cache<?, ?>> {

   private final String cacheName;
   private final String templateName;
   private final EmbeddedCacheManager cacheManager;

   public GetOrCreateCacheWithTemplateNameAction(EmbeddedCacheManager cacheManager, String cacheName, String templateName) {
      this.cacheManager = cacheManager;
      this.cacheName = cacheName;
      this.templateName = templateName;
   }

   @Override
   public Cache<?, ?> run() {
      return cacheManager.administration().getOrCreateCache(cacheName, templateName);
   }
}
