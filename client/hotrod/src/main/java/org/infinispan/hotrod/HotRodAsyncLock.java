package org.infinispan.hotrod;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.api.async.AsyncLock;

/**
 * @since 14.0
 **/
public class HotRodAsyncLock implements AsyncLock {
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
   public HotRodAsyncContainer container() {
      return hotrod.async();
   }

   @Override
   public CompletionStage<Void> lock() {
      return null;
   }

   @Override
   public CompletionStage<Boolean> tryLock() {
      return null;
   }

   @Override
   public CompletionStage<Boolean> tryLock(long time, TimeUnit unit) {
      return null;
   }

   @Override
   public CompletionStage<Void> unlock() {
      return null;
   }

   @Override
   public CompletionStage<Boolean> isLocked() {
      return null;
   }

   @Override
   public CompletionStage<Boolean> isLockedByMe() {
      return null;
   }
}
