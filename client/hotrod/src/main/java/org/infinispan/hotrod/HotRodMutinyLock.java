package org.infinispan.hotrod;

import java.util.concurrent.TimeUnit;

import org.infinispan.api.mutiny.MutinyLock;

import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public class HotRodMutinyLock implements MutinyLock {
   private final HotRod hotrod;
   private final String name;

   HotRodMutinyLock(HotRod hotrod, String name) {
      this.hotrod = hotrod;
      this.name = name;
   }

   @Override
   public String name() {
      return name;
   }

   @Override
   public HotRodMutinyContainer container() {
      return hotrod.mutiny();
   }

   @Override
   public Uni<Void> lock() {
      return null;
   }

   @Override
   public Uni<Boolean> tryLock() {
      return null;
   }

   @Override
   public Uni<Boolean> tryLock(long time, TimeUnit unit) {
      return null;
   }

   @Override
   public Uni<Void> unlock() {
      return null;
   }

   @Override
   public Uni<Boolean> isLocked() {
      return null;
   }

   @Override
   public Uni<Boolean> isLockedByMe() {
      return null;
   }
}
