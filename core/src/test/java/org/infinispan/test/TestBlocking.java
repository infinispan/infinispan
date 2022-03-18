package org.infinispan.test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Class used for various utility methods to encapsulate a blocking invocation. This is useful for tests as all
 * methods in this class are automatically excluded from any non blocking verification done by BlockHound.
 */
public class TestBlocking {
   private TestBlocking() { }

   public static <I> I exchange(Exchanger<I> exchanger, I value, long time, TimeUnit timeUnit)
         throws InterruptedException, TimeoutException {
      return exchanger.exchange(value, time, timeUnit);
   }

   public static boolean await(CountDownLatch latch, long time, TimeUnit timeUnit) throws InterruptedException {
      return latch.await(time, timeUnit);
   }

   public static void await(CyclicBarrier barrier, long time, TimeUnit timeUnit) throws InterruptedException,
         TimeoutException, BrokenBarrierException {
      barrier.await(time, timeUnit);
   }

   public static <V> V get(Future<V> future, long time, TimeUnit timeUnit) throws ExecutionException,
         InterruptedException, TimeoutException {
      return future.get(time, timeUnit);
   }
}
