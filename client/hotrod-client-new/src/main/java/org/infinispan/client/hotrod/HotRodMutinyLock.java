package org.infinispan.client.hotrod;

import java.util.concurrent.TimeUnit;

import org.infinispan.api.Experimental;
import org.infinispan.api.mutiny.MutinyContainer;
import org.infinispan.api.mutiny.MutinyLock;

import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodMutinyLock implements MutinyLock {
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
   public MutinyContainer container() {
      return hotrod.mutiny();
   }

   @Override
   public Uni<Void> lock() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Boolean> tryLock() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Boolean> tryLock(long time, TimeUnit unit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Void> unlock() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Boolean> isLocked() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Boolean> isLockedByMe() {
      throw new UnsupportedOperationException();
   }
}
