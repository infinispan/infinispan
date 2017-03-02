package org.infinispan.interceptors.distribution;

import java.util.concurrent.CompletableFuture;

/**
 * A {@link Collector} implementation that only waits for the primary owner.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class PrimaryOwnerOnlyCollector<T> implements Collector<T> {

   private final CompletableFuture<T> future;

   public PrimaryOwnerOnlyCollector() {
      future = new CompletableFuture<>();
   }

   public CompletableFuture<T> getFuture() {
      return future;
   }

   @Override
   public void primaryException(Throwable throwable) {
      future.completeExceptionally(throwable);
   }

   @Override
   public void primaryResult(T result, boolean success) {
      future.complete(result);
   }
}
