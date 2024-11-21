package org.infinispan.client.hotrod;

import java.util.concurrent.TimeUnit;

import org.infinispan.api.Experimental;
import org.infinispan.api.sync.SyncContainer;
import org.infinispan.api.sync.SyncLock;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodSyncLock implements SyncLock {
   private final HotRod hotrod;
   private final String name;

   HotRodSyncLock(HotRod hotrod, String name) {
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
   public void lock() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean tryLock() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean tryLock(long time, TimeUnit unit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void unlock() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isLocked() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isLockedByMe() {
      throw new UnsupportedOperationException();
   }
}
