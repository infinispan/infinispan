package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.sync.SyncWeakCounter;
import org.infinispan.api.sync.SyncWeakCounters;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodSyncWeakCounters implements SyncWeakCounters {
   private final HotRod hotrod;

   HotRodSyncWeakCounters(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public SyncWeakCounter get(String name) {
      return new HotRodSyncWeakCounter(hotrod, name); // PLACEHOLDER
   }

   @Override
   public SyncWeakCounter create(String name, CounterConfiguration counterConfiguration) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void remove(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Iterable<String> names() {
      throw new UnsupportedOperationException();
   }
}
