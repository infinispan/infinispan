package org.infinispan.test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class JREBlocking {
   private JREBlocking() { }

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
}
