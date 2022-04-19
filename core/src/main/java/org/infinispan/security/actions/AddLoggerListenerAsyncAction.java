package org.infinispan.security.actions;

import java.util.concurrent.CompletionStage;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.events.EventLogManager;

/**
 * Add logger listener action.
 *
 * @since 14.0
 */
public class AddLoggerListenerAsyncAction extends AbstractEmbeddedCacheManagerAction<CompletionStage<Void>> {
   private final Object listener;

   public AddLoggerListenerAsyncAction(EmbeddedCacheManager cacheManager, Object listener) {
      super(cacheManager);
      this.listener = listener;
   }

   @Override
   public CompletionStage<Void> run() {
      return EventLogManager.getEventLogger(cacheManager).addListenerAsync(listener);
   }
}
