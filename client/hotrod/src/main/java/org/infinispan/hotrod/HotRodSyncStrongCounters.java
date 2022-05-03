package org.infinispan.hotrod;

import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.sync.SyncStrongCounters;

/**
 * @since 14.0
 **/
public class HotRodSyncStrongCounters implements SyncStrongCounters {
   private final HotRod hotrod;

   public HotRodSyncStrongCounters(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public HotRodSyncStrongCounter get(String name) {
      return new HotRodSyncStrongCounter(hotrod, name); // PLACEHOLDER
   }

   @Override
   public HotRodSyncStrongCounter create(String name, CounterConfiguration counterConfiguration) {
      return new HotRodSyncStrongCounter(hotrod, name); // PLACEHOLDER
   }

   @Override
   public void remove(String name) {

   }

   @Override
   public Iterable<String> names() {
      return null;
   }
}
