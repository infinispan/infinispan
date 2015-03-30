package org.infinispan.executors;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertSame;

/**
 * Basic tests for {@link SemaphoreCompletionService}
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "executors.SemaphoreCompletionServiceTest")
public class SemaphoreCompletionServiceTest extends AbstractInfinispanTest {
   private final ExecutorService executor2Threads = Executors.newFixedThreadPool(2, getTestThreadFactory("Test"));

   @AfterClass(alwaysRun = true)
   public void stopExecutors() {
      executor2Threads.shutdownNow();
   }

   public void testConcurrency1WithinThread() throws Exception {
      SemaphoreCompletionService<String> completionService = new SemaphoreCompletionService<>(new WithinThreadExecutor(), 1);


      Future<String> future1 = completionService.submit(new DummyTask());
      Future<String> future2 = completionService.poll();
      assertSame(future1, future2);
      assertNotNull(future2);
      assertEquals("bla", future2.get());
   }

   public void testConcurrencyLimit() throws Exception {
      SemaphoreCompletionService<String> completionService = new SemaphoreCompletionService<>(executor2Threads, 1);
      CountDownLatch latch = new CountDownLatch(1);

      Future<String> blockingFuture = completionService.submit(new BlockingTask(latch));

      Future<String> dummyFuture = completionService.submit(new DummyTask());
      assertNull(completionService.poll(1, SECONDS));
      assertFalse(dummyFuture.isDone());

      latch.countDown();
      assertEquals("bla", blockingFuture.get(10, SECONDS));
      assertEquals("bla", dummyFuture.get(10, SECONDS));
   }

   public void testBackgroundTasks() throws Exception {
      SemaphoreCompletionService<String> completionService = new SemaphoreCompletionService<>(executor2Threads, 1);
      CountDownLatch latch = new CountDownLatch(1);

      Future<String> backgroundInitFuture = completionService.submit(new BackgroundInitTask(completionService));
      assertEquals("bla", backgroundInitFuture.get(1, SECONDS));

      Future<String> dummyFuture = completionService.submit(new DummyTask());
      assertSame(backgroundInitFuture, completionService.poll(1, SECONDS));
      assertFalse(dummyFuture.isDone());

      Future<String> backgroundEndFuture = completionService.backgroundTaskFinished(new BlockingTask(latch));
      assertNull(completionService.poll(1, SECONDS));
      assertFalse(dummyFuture.isDone());

      latch.countDown();
      assertEquals("bla", backgroundEndFuture.get(10, SECONDS));
      assertEquals("bla", dummyFuture.get(10, SECONDS));
   }


   private static class DummyTask implements Callable<String> {
      @Override
      public String call() throws Exception {
         return "bla";
      }
   }

   private static class BlockingTask implements Callable<String> {
      private final CountDownLatch latch;

      private BlockingTask(CountDownLatch latch) {
         this.latch = latch;
      }

      @Override
      public String call() throws Exception {
         latch.await(30, SECONDS);
         return "bla";
      }
   }

   private static class BackgroundInitTask implements Callable<String> {
      private final SemaphoreCompletionService<String> completionService;

      private BackgroundInitTask(SemaphoreCompletionService<String> completionService) {
         this.completionService = completionService;
      }

      @Override
      public String call() throws Exception {
         completionService.continueTaskInBackground();
         return "bla";
      }
   }
}
