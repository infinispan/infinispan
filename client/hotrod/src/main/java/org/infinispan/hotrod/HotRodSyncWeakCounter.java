package org.infinispan.hotrod;

import org.infinispan.api.sync.SyncWeakCounter;

/**
 * @since 14.0
 **/
public class HotRodSyncWeakCounter implements SyncWeakCounter {
   private final HotRod hotrod;
   private final String name;

   HotRodSyncWeakCounter(HotRod hotrod, String name) {
      this.hotrod = hotrod;
      this.name = name;
   }

   @Override
   public String name() {
      return null;
   }

   @Override
   public HotRodSyncContainer container() {
      return hotrod.sync();
   }

   @Override
   public long value() {
      return 0;
   }

   @Override
   public void add(long delta) {

   }
}
