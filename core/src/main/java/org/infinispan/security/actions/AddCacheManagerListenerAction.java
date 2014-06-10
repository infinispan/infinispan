package org.infinispan.security.actions;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * CacheManagerAddListenerAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AddCacheManagerListenerAction extends AbstractEmbeddedCacheManagerAction<Void> {

   private final Object listener;

   public AddCacheManagerListenerAction(EmbeddedCacheManager cacheManager, Object listener) {
      super(cacheManager);
      this.listener = listener;
   }

   @Override
   public Void run() {
      cacheManager.addListener(listener);
      return null;
   }

}
