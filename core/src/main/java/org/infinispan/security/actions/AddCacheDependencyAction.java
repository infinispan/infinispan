package org.infinispan.security.actions;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * AddCacheDependencyAction.
 *
 * @author Dan Berindei
 * @since 10.0
 */
public class AddCacheDependencyAction extends AbstractEmbeddedCacheManagerAction<Void> {

   private final String from;
   private final String to;

   public AddCacheDependencyAction(EmbeddedCacheManager cacheManager, String from, String to) {
      super(cacheManager);
      this.from = from;
      this.to = to;
   }

   @Override
   public Void get() {
      cacheManager.addCacheDependency(from, to);
      return null;
   }
}
