package org.infinispan.hotrod;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncLock;
import org.infinispan.api.async.AsyncLocks;
import org.infinispan.api.configuration.LockConfiguration;

/**
 * @since 14.0
 **/
public class HotRodAsyncLocks implements AsyncLocks {
   private final HotRod hotrod;

   HotRodAsyncLocks(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public CompletionStage<AsyncLock> create(String name, LockConfiguration configuration) {
      return CompletableFuture.completedFuture(new HotRodAsyncLock(hotrod, name)); // PLACEHOLDER
   }

   @Override
   public CompletionStage<AsyncLock> lock(String name) {
      return CompletableFuture.completedFuture(new HotRodAsyncLock(hotrod, name)); // PLACEHOLDER
   }

   @Override
   public CompletionStage<Void> remove(String name) {
      return null;
   }

   @Override
   public Flow.Publisher<String> names() {
      return null;
   }
}
