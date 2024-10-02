package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.sync.SyncStrongCounter;
import org.infinispan.api.sync.SyncStrongCounters;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodSyncStrongCounters implements SyncStrongCounters {
   private final HotRod hotrod;

   HotRodSyncStrongCounters(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public SyncStrongCounter get(String name) {
      return new HotRodSyncStrongCounter(hotrod, name); // PLACEHOLDER
   }

   @Override
   public SyncStrongCounter create(String name, CounterConfiguration counterConfiguration) {
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
