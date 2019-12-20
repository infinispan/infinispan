package org.infinispan.server.infinispan.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.eviction.impl.PassivationManager;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * FlushCacheAction.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class FlushCacheAction extends AbstractAdvancedCacheAction<Void> {
   public FlushCacheAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public Void run() {
      PassivationManager passivationManager = cache.getComponentRegistry().getComponent(PassivationManager.class);
      if (passivationManager != null) {
          CompletionStages.join(passivationManager.passivateAllAsync());
      }
      return null;
   }
}
