package org.infinispan.commons.util.concurrent;

import org.infinispan.commons.CacheException;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertFalse;

/**
 * Tests for NotifyingFuture composition
 *
 * @author gustavonalle
 * @since 7.1
 */
@Test(groups = "unit", testName = "commons.FuturesTest")
public class FuturesTest extends AbstractInfinispanTest {

   @Test
   public void testCombineWithQuickCompletingFutures() throws Exception {
      List<NotifyingFuture<Integer>> futures = new ArrayList<>();

      Random random = new Random();

      for (int i = 0; i < 100; i++) {
         futures.add(createDelayedFuture(i, random.nextInt(25)));
      }

      final NotifyingFuture<List<Integer>> compositeFuture = Futures.combine(futures);

      final CountDownLatch endLatch = new CountDownLatch(1);

      compositeFuture.attachListener(new FutureListener<List<Integer>>() {
         @Override
         public void futureDone(Future<List<Integer>> future) {
            assertTrue(future.isDone());
            assertFalse(future.isCancelled());
            endLatch.countDown();
         }
      });
      endLatch.await();

      assertTrue(compositeFuture.isDone());
      assertAllDone(futures);
   }

   @Test
   public void testCombineWithMultipleListeners() throws Exception {
      List<NotifyingFuture<Integer>> futures = new ArrayList<>();

      Random random = new Random();
      for (int i = 0; i < 10; i++) {
         futures.add(createDelayedFuture(i, random.nextInt(20)));
      }

      NotifyingFuture<List<Integer>> compositeFuture = Futures.combine(futures);

      final AtomicInteger fireCount = new AtomicInteger(0);
      final CountDownLatch listenersDone = new CountDownLatch(100);
      for (int i = 0; i < 100; i++) {
         compositeFuture.attachListener(new FutureListener<List<Integer>>() {
            @Override
            public void futureDone(Future<List<Integer>> future) {
               assertTrue(future.isDone());
               fireCount.incrementAndGet();
               listenersDone.countDown();
            }
         });
      }
      listenersDone.await();

      assertEquals(fireCount.get(), 100);
      assertTrue(compositeFuture.isDone());
      assertFalse(compositeFuture.isCancelled());
      assertAllDone(futures);
   }

   @Test
   public void testCombineWithImmediateFutures() throws Exception {
      List<NotifyingFuture<String>> futures = new ArrayList<>();
      final CountDownLatch startLatch = new CountDownLatch(1);
      final CountDownLatch endLatch = new CountDownLatch(1);

      for (int i = 0; i < 100; i++) {
         futures.add(createImmediate("res" + i, startLatch));
      }

      NotifyingFuture<List<String>> compositeFuture = Futures.combine(futures);

      final AtomicInteger triggerCount = new AtomicInteger(0);
      compositeFuture.attachListener(new FutureListener<List<String>>() {
         @Override
         public void futureDone(Future<List<String>> future) {
            assertTrue(future.isDone());
            assertFalse(future.isCancelled());
            assertNoErrors(future);
            try {
               List<String> result = future.get();
               for (int i = 0; i < 100; i++) {
                  assertTrue(result.contains("res" + 1));
               }
            } catch (InterruptedException | ExecutionException ignored) {}
            triggerCount.incrementAndGet();
            endLatch.countDown();
         }
      });

      startLatch.countDown();
      endLatch.await();

      assertEquals(1, triggerCount.get());
      assertTrue(compositeFuture.isDone());
      assertFalse(compositeFuture.isCancelled());
      assertAllDone(futures);
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testCombineWithTimeout() throws Exception {
      List<NotifyingFuture<String>> futures = new ArrayList<>();

      futures.add(createNeverCompletingFuture("ignored"));
      futures.add(createDelayedFuture("ignored", 40));

      NotifyingFuture<?> compositeFuture = Futures.combine(futures);

      compositeFuture.get(20, TimeUnit.MILLISECONDS);
   }

   @Test(expectedExceptions = ExecutionException.class)
   public void testCombineWithCompletionErrors() throws Exception {
      List<NotifyingFuture<Void>> futures = new ArrayList<>();

      futures.add(createFutureWithError(new CacheException()));
      futures.add(createFutureWithError(new CacheException()));

      NotifyingFuture<?> compositeFuture = Futures.combine(futures);

      compositeFuture.get();
   }

   @Test(expectedExceptions = CancellationException.class)
   public void testCancellation() throws ExecutionException, InterruptedException, TimeoutException {
      List<NotifyingFuture<String>> futures = new ArrayList<>();

      futures.add(createNeverCompletingFuture("ignored"));
      futures.add(createNeverCompletingFuture("ignored2"));
      futures.add(createNeverCompletingFuture("ignored3"));

      NotifyingFuture<?> composite = Futures.combine(futures);

      composite.cancel(true);

      assertTrue(composite.isCancelled());
      assertAllDone(futures);

      composite.get();
   }

   @Test
   public void testAndThen() throws ExecutionException, InterruptedException {
      NotifyingFuture<Double> aFuture = createDelayedFuture(42.0, 50);
      final AtomicBoolean wasCalled = new AtomicBoolean(false);
      Runnable aTask = new Runnable() {
         @Override
         public void run() {
            wasCalled.set(true);
         }
      };

      NotifyingFuture<Void> chained = Futures.andThen(aFuture, aTask);

      chained.get();

      assertTrue(wasCalled.get());
   }

   @Test
   public void testMultiChained() throws ExecutionException, InterruptedException {
      final List<NotifyingFuture<Integer>> futures = new ArrayList<>();
      futures.add(createDelayedFuture(2, 50));
      futures.add(createDelayedFuture(4, 50));
      futures.add(createDelayedFuture(6, 50));

      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicBoolean wasCalled = new AtomicBoolean(false);
      NotifyingFuture<Void> chained = Futures.andThen(Futures.combine(futures), new Runnable() {
         @Override
         public void run() {
            wasCalled.set(true);
         }
      });

      chained.attachListener(new FutureListener<Void>() {
         @Override
         public void futureDone(Future<Void> future) {
            latch.countDown();
         }
      });
      latch.await();

      assertTrue(chained.isDone());
      assertTrue(wasCalled.get());
      assertFalse(chained.isCancelled());
   }

   @Test(expectedExceptions = CancellationException.class)
   public void testAndThenWithCancellation() throws ExecutionException, InterruptedException {
      final List<NotifyingFuture<Double>> futures = new ArrayList<>();
      futures.add(createNeverCompletingFuture(42.0));
      futures.add(createNeverCompletingFuture(42.0));

      NotifyingFuture<?> combined = Futures.combine(futures);

      NotifyingFuture<Void> andThenFuture = Futures.andThen(combined, new Runnable() {
         @Override
         public void run() {
            fail("Should not call afterTask if initial future is cancelled");
         }
      });

      andThenFuture.cancel(true);

      assertTrue(andThenFuture.isCancelled());
      andThenFuture.get();
   }

   @Test(expectedExceptions = ExecutionException.class)
   public void testAndThenErrorPropagation() throws ExecutionException, InterruptedException {

      NotifyingFuture<?> combined = Futures.combine(Arrays.asList(createFutureWithError(new CacheException())));

      NotifyingFuture<Void> andThenFuture = Futures.andThen(combined, new Runnable() {
         @Override
         public void run() {
            fail("Should not call afterTask if initial future has errors");
         }
      });

      andThenFuture.get();
   }

   @Test
   public void testAndThenWithListener() throws ExecutionException, InterruptedException {
      NotifyingFuture<Double> future = createDelayedFuture(42.0, 20);

      final AtomicBoolean taskRun = new AtomicBoolean();
      final CountDownLatch futureLatch = new CountDownLatch(1);

      NotifyingFuture<Void> andThenFuture = Futures.andThen(future, new Runnable() {
         @Override
         public void run() {
            taskRun.set(true);
         }
      });

      andThenFuture.attachListener(new FutureListener<Void>() {
         @Override
         public void futureDone(Future<Void> future) {
            futureLatch.countDown();
         }
      });

      futureLatch.await();

      assertTrue(taskRun.get());
   }

   private void assertNoErrors(Future<?> future) {
      try {
         future.get();
      } catch (Exception e) {
         fail("Should not have errors");
      }
   }

   private <T> void assertAllDone(List<NotifyingFuture<T>> futures) {
      for (Future<T> future : futures) {
         assertTrue(future.isDone());
      }
   }

   private <T> NotifyingFuture<T> createDelayedFuture(final T result, final long afterHowLong) {
      final NotifyingFutureImpl<T> notifyingFuture = new NotifyingFutureImpl<>();
      Future<T> future = fork(new Callable<T>() {
         @Override
         public T call() throws Exception {
            Thread.sleep(afterHowLong);
            notifyingFuture.notifyDone(result);
            return result;
         }
      });
      notifyingFuture.setFuture(future);
      return notifyingFuture;
   }

   private <T> NotifyingFuture<T> createImmediate(final T result, final CountDownLatch startLatch) {
      final NotifyingFutureImpl<T> notifyingFuture = new NotifyingFutureImpl<>();
      Future<T> future = fork(new Callable<T>() {
         @Override
         public T call() throws Exception {
            startLatch.countDown();
            notifyingFuture.notifyDone(result);
            return result;
         }
      });
      notifyingFuture.setFuture(future);
      return notifyingFuture;
   }

   private <T> NotifyingFuture<T> createNeverCompletingFuture(final T result) {
      final NotifyingFutureImpl<T> notifyingFuture = new NotifyingFutureImpl<>();
      Future<T> future = fork(new Callable<T>() {
         @Override
         public T call() throws Exception {
            LockSupport.park();
            return result;
         }
      });
      notifyingFuture.setFuture(future);
      return notifyingFuture;
   }

   @SuppressWarnings("unchecked")
   private NotifyingFuture<Void> createFutureWithError(final Exception exception) {
      final NotifyingFutureImpl<Void> notifyingFuture = new NotifyingFutureImpl<>();
      Future future = fork(new Runnable() {
         @Override
         public void run() {
            notifyingFuture.notifyException(exception);
         }
      });
      notifyingFuture.setFuture(future);
      return notifyingFuture;
   }


}
