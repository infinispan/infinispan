package org.infinispan.embedded;

import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.sync.SyncStrongCounter;
import org.infinispan.api.sync.SyncStrongCounters;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.StrongCounter;

/**
 * @since 15.0
 */
public class EmbeddedSyncStrongCounters implements SyncStrongCounters {
   private final Embedded embedded;
   private final CounterManager counterManager;

   EmbeddedSyncStrongCounters(Embedded embedded) {
      this.embedded = embedded;
      this.counterManager = EmbeddedCounterManagerFactory.asCounterManager(embedded.cacheManager);
   }

   @Override
   public SyncStrongCounter get(String name) {
      StrongCounter counter = counterManager.getStrongCounter(name);
      return new EmbeddedSyncStrongCounter(embedded, counter);
   }

   @Override
   public SyncStrongCounter create(String name, CounterConfiguration counterConfiguration) {
      return null;
   }

   @Override
   public void remove(String name) {
      counterManager.remove(name);
   }

   @Override
   public Iterable<String> names() {
      //FIXME: filter strong counters
      return counterManager.getCounterNames();
   }
}
