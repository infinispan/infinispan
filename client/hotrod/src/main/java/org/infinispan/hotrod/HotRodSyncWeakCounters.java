package org.infinispan.hotrod;

import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.sync.SyncWeakCounter;
import org.infinispan.api.sync.SyncWeakCounters;

/**
 * @since 14.0
 **/
public class HotRodSyncWeakCounters implements SyncWeakCounters {
   private final HotRod hotrod;

   public HotRodSyncWeakCounters(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public SyncWeakCounter get(String name) {
      return new HotRodSyncWeakCounter(hotrod, name); // PLACEHOLDER
   }

   @Override
   public SyncWeakCounter create(String name, CounterConfiguration counterConfiguration) {
      return new HotRodSyncWeakCounter(hotrod, name); // PLACEHOLDER
   }

   @Override
   public void remove(String name) {

   }

   @Override
   public Iterable<String> names() {
      return null;
   }
}
