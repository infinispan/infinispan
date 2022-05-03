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
   public static <T> void assertAwaitEquals(T expected, CompletionStage<T> actualFuture) {
      T actual = await(actualFuture);
      Assertions.assertEquals(expected, actual);
   }

   public static <T> T await(CompletionStage<T> actual) {
      CompletableFuture<T> future = actual.toCompletableFuture();
      boolean completed = CompletableFutures.uncheckedAwait(future, 30, TimeUnit.SECONDS);
      if (!completed) {
         Assertions.fail("Timeout obtaining responses");
      }
      return future.getNow(null);
   }
}
