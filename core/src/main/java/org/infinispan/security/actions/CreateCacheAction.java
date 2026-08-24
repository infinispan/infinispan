package org.infinispan.security.actions;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.impl.InternalCacheManager;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * Registers and starts a cache on the given cache manager, offloading the start to the supplied
 * {@link BlockingManager}. Registration is synchronous; the returned stage completes when the cache
 * has started.
 *
 * @author José Bolina
 * @since 16.4
 */
public class CreateCacheAction implements Supplier<CompletionStage<Void>> {

   private final EmbeddedCacheManager ecm;
   private final String cacheName;
   private final BlockingManager blockingManager;

   public CreateCacheAction(EmbeddedCacheManager ecm, String cacheName, BlockingManager blockingManager) {
      this.ecm = ecm;
      this.cacheName = cacheName;
      this.blockingManager = blockingManager;
   }

   @Override
   public CompletionStage<Void> get() {
      return InternalCacheManager.createCache(ecm, cacheName, blockingManager);
   }
}
