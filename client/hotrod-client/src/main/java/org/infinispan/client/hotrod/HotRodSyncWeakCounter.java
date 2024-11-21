package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.api.sync.SyncContainer;
import org.infinispan.api.sync.SyncWeakCounter;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodSyncWeakCounter implements SyncWeakCounter {
   private final HotRod hotrod;
   private final String name;

   HotRodSyncWeakCounter(HotRod hotrod, String name) {
      this.hotrod = hotrod;
      this.name = name;
   }

   @Override
   public String name() {
      return name;
   }

   @Override
   public SyncContainer container() {
      return hotrod.sync();
   }

   @Override
   public long value() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void add(long delta) {
      throw new UnsupportedOperationException();
   }
}
