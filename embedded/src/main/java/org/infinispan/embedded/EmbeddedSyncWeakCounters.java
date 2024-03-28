package org.infinispan.embedded;

import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.sync.SyncWeakCounter;
import org.infinispan.api.sync.SyncWeakCounters;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.WeakCounter;

/**
 * @since 15.0
 */
public class EmbeddedSyncWeakCounters implements SyncWeakCounters {
   private final Embedded embedded;
   private final CounterManager counterManager;

   EmbeddedSyncWeakCounters(Embedded embedded) {
      this.embedded = embedded;
      this.counterManager = EmbeddedCounterManagerFactory.asCounterManager(embedded.cacheManager);
   }

   @Override
   public SyncWeakCounter get(String name) {
      WeakCounter counter = counterManager.getWeakCounter(name);
      return new EmbeddedSyncWeakCounter(embedded, counter);
   }

   @Override
   public SyncWeakCounter create(String name, CounterConfiguration counterConfiguration) {
      return null;
   }

   @Override
   public void remove(String name) {

   }

   @Override
   public Iterable<String> names() {
      return null;
   }
}
