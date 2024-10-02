package org.infinispan.client.hotrod;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.api.Experimental;
import org.infinispan.api.async.AsyncContainer;
import org.infinispan.api.async.AsyncLock;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodAsyncLock implements AsyncLock {
   private final HotRod hotrod;
   private final String name;

   HotRodAsyncLock(HotRod hotrod, String name) {
      this.hotrod = hotrod;
      this.name = name;
   }

   @Override
   public String name() {
      return name;
   }

   @Override
   public AsyncContainer container() {
      return hotrod.async();
   }

   @Override
   public CompletionStage<Void> lock() {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Boolean> tryLock() {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Boolean> tryLock(long time, TimeUnit unit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Void> unlock() {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Boolean> isLocked() {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Boolean> isLockedByMe() {
      throw new UnsupportedOperationException();
   }
}
