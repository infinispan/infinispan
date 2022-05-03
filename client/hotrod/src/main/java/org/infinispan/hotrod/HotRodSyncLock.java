package org.infinispan.hotrod;

import java.util.concurrent.TimeUnit;

import org.infinispan.api.sync.SyncLock;

/**
 * @since 14.0
 **/
public class HotRodSyncLock implements SyncLock {
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
   public HotRodSyncContainer container() {
      return hotrod.sync();
   }

   @Override
   public void lock() {

   }

   @Override
   public boolean tryLock() {
      return false;
   }

   @Override
   public boolean tryLock(long time, TimeUnit unit) {
      return false;
   }

   @Override
   public void unlock() {

   }

   @Override
   public boolean isLocked() {
      return false;
   }

   @Override
   public boolean isLockedByMe() {
      return false;
   }
}
