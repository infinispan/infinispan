package org.infinispan.assertions;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.Assert.*;

/**
 * Custom assertion for {@link org.infinispan.commons.util.concurrent.NotifyingFuture}
 *
 * @author Sebastian Laskawiec
 */
public class FutureAssertion<T> {

   private int futureGetTimeoutMs;

   private Future<T> actual;

   private FutureAssertion(Future<T> actual, int futureGetTimeoutMs) {
      this.actual = actual;
      this.futureGetTimeoutMs = futureGetTimeoutMs;
   }

   public static <T> FutureAssertion<T> assertThat(Future<T> actual, int futureGetTimeoutMs) {
      return new FutureAssertion<>(actual, futureGetTimeoutMs);
   }

   public FutureAssertion<T> isDone() {
      assertTrue(actual.isDone());
      return this;
   }

   public FutureAssertion<T> isNotCanceled() {
      assertFalse(actual.isCancelled());
      return this;
   }

   public FutureAssertion<T> hasValue(T value) throws ExecutionException, InterruptedException, TimeoutException {
      assertEquals(actual.get(futureGetTimeoutMs, TimeUnit.MILLISECONDS), value);
      return this;
   }
}
