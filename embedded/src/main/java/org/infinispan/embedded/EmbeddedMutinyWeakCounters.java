package org.infinispan.embedded;

import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.mutiny.MutinyWeakCounter;
import org.infinispan.api.mutiny.MutinyWeakCounters;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterManager;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 15.0
 */
public class EmbeddedMutinyWeakCounters implements MutinyWeakCounters {
   private final Embedded embedded;
   private final CounterManager counterManager;

   EmbeddedMutinyWeakCounters(Embedded embedded) {
      this.embedded = embedded;
      this.counterManager = EmbeddedCounterManagerFactory.asCounterManager(embedded.cacheManager);
   }

   @Override
   public Uni<MutinyWeakCounter> get(String name) {
      return null;
   }

   @Override
   public Uni<MutinyWeakCounter> create(String name, CounterConfiguration configuration) {
      return null;
   }

   @Override
   public Uni<Void> remove(String name) {
      return null;
   }

   @Override
   public Multi<String> names() {
      return null;
   }
}
