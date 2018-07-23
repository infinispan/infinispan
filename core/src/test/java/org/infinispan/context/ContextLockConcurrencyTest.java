package org.infinispan.context;

import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.context.impl.ContextLock;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.Exceptions;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = {"unit", "stress"}, testName = "context.ContextLockConcurrencyTest")
public class ContextLockConcurrencyTest extends AbstractInfinispanTest {
   CompletableFuture exceptionFuture = new CompletableFuture();
   ExecutorService executor = Executors.newFixedThreadPool(8);
   volatile boolean quit = false;
   Context context = new Context();
   volatile Thread critical;

   @AfterClass(alwaysRun = true)
   public void cleanup() {
      executor.shutdown();
   }

   public void test() {
      List<Future<?>> futures = IntStream.range(0, 8).mapToObj(i -> executor.submit(() -> runThread(i))).collect(Collectors.toList());
      Exceptions.expectException(TimeoutException.class, () -> exceptionFuture.get(60, TimeUnit.SECONDS));
      quit = true;
      futures.forEach(future -> {
         try {
            future.get(10, TimeUnit.SECONDS);
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      });
   }

   private void runThread(int id) {
      AtomicInteger inCritical = new AtomicInteger(0);
      try {
         while (!quit) {
//            System.out.printf("%d/%s entering%n", id, Thread.currentThread());
            CompletionStage<Void> cs = ContextLock.enter(context, Context.updater);
            if (cs == null) {
//               System.out.printf("%d/%s entered%n", id, Thread.currentThread());
               critical(inCritical);
//               System.out.printf("%d/%s exiting%n", id, Thread.currentThread());
               ContextLock.exit(context, Context.updater);
//               System.out.printf("%d/%s exited%n", id, Thread.currentThread());
            } else {
//               System.out.printf("%d/%s waits for %s%n", id, Thread.currentThread(), cs);
               CountDownLatch latch = new CountDownLatch(1);
               cs.thenAccept(nil -> {
//                  System.out.printf("%d/%s entered async%n", id, Thread.currentThread());
                  critical(inCritical);
//                  System.out.printf("%d/%s exiting async%n", id, Thread.currentThread());
                  ContextLock.exit(context, Context.updater);
//                  System.out.printf("%d/%s exited async%n", id, Thread.currentThread());
                  latch.countDown();
               });
               try {
                  assertTrue(latch.await(10, TimeUnit.SECONDS));
               } catch (InterruptedException e) {
                  throw new IllegalStateException(e);
               }
            }
         }
      } catch (Throwable e) {
         exceptionFuture.completeExceptionally(e);
      }
      System.out.printf("Thread %d/%s in critical section %d times%n", id, Thread.currentThread(), inCritical.get());
   }

   private void critical(AtomicInteger inCritical) {
      assert critical == null : "Thread " + critical + " in critical section";
      critical = Thread.currentThread();
      inCritical.incrementAndGet();
//      Thread.yield();
      TestingUtil.sleepThread(1);
      critical = null;
   }

   static class Context {
      private static final AtomicReferenceFieldUpdater<Context, Object> updater = AtomicReferenceFieldUpdater.newUpdater(Context.class, Object.class, "lock");
      private volatile Object lock;
   }
}
