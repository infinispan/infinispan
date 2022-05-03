package org.infinispan.hotrod;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.junit.jupiter.api.Assertions;

/**
 * @since 14.0
 **/
public class AwaitAssertions {
   public static <T> void assertEquals(T expected, CompletionStage<T> actual) {
      CompletableFuture<T> future = actual.toCompletableFuture();
      boolean completed = CompletableFutures.uncheckedAwait(future, 30, TimeUnit.SECONDS);
      if (!completed) {
         Assertions.fail("Timeout obtaining responses");
      }
      Assertions.assertEquals(expected, future.getNow(null));
   }
}
