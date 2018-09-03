package org.infinispan.counter;

import static java.util.Objects.requireNonNull;

import org.infinispan.counter.api.CounterManager;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * A {@link CounterManager} factory for embedded cached.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public final class EmbeddedCounterManagerFactory {

   private EmbeddedCounterManagerFactory() {
   }

   /**
    * @return the {@link CounterManager} associated to the {@link EmbeddedCacheManager}.
    */
   public static CounterManager asCounterManager(EmbeddedCacheManager cacheManager) {
      return requireNonNull(cacheManager, "EmbeddedCacheManager can't be null.")
                .getGlobalComponentRegistry()
                .getComponent(BasicComponentRegistry.class)
                .getComponent(CounterManager.class)
                .running();
   }
}
