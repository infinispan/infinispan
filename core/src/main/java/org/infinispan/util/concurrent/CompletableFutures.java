package org.infinispan.util.concurrent;

import org.infinispan.commons.util.concurrent.NotifyingNotifiableFuture;

import java.util.concurrent.CompletableFuture;

/**
 * Utility methods connecting {@link CompletableFuture} futures and our {@link NotifyingNotifiableFuture} futures.
 *
 * @author Dan Berindei
 * @since 8.0
 */
public class CompletableFutures {
   public static <T> void connect(NotifyingNotifiableFuture<T> sink, CompletableFuture<T> source) {
      CompletableFuture<T> compoundSource = source.whenComplete((value, throwable) -> {
         if (throwable == null) {
            sink.notifyDone(value);
         } else {
            sink.notifyException(throwable);
         }
      });
      sink.setFuture(compoundSource);
   }
}
