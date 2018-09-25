package org.infinispan.counter;

import static java.util.Objects.requireNonNull;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.lifecycle.ComponentStatus;
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
    * @throws IllegalLifecycleStateException if the cache manager is not running
    */
   public static CounterManager asCounterManager(EmbeddedCacheManager cacheManager) {
      requireNonNull(cacheManager, "EmbeddedCacheManager can't be null.");

      if (cacheManager.getStatus() != ComponentStatus.RUNNING)
         throw new IllegalLifecycleStateException();

      return cacheManager.getGlobalComponentRegistry()
                            .getComponent(BasicComponentRegistry.class)
                            .getComponent(CounterManager.class)
                            .running();
   }
}
