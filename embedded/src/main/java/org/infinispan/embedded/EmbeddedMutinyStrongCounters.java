package org.infinispan.embedded;

import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.mutiny.MutinyStrongCounter;
import org.infinispan.api.mutiny.MutinyStrongCounters;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterManager;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 15.0
 */
public class EmbeddedMutinyStrongCounters implements MutinyStrongCounters {
   private final Embedded embedded;
   private final CounterManager counterManager;

   EmbeddedMutinyStrongCounters(Embedded embedded) {
      this.embedded = embedded;
      this.counterManager = EmbeddedCounterManagerFactory.asCounterManager(embedded.cacheManager);
   }

   @Override
   public Uni<MutinyStrongCounter> get(String name) {
      return null;
   }

   @Override
   public Uni<MutinyStrongCounter> create(String name, CounterConfiguration configuration) {
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
