package org.infinispan.security.actions;

import java.util.concurrent.CompletionStage;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * CacheManagerAddListenerAction.
 *
 * @since 14.0
 */
public class AddCacheManagerListenerAsyncAction extends AbstractEmbeddedCacheManagerAction<CompletionStage<Void>> {

   private final Object listener;

   public AddCacheManagerListenerAsyncAction(EmbeddedCacheManager cacheManager, Object listener) {
      super(cacheManager);
      this.listener = listener;
   }

   @Override
   public CompletionStage<Void> run() {
      return cacheManager.addListenerAsync(listener);
   }

}
