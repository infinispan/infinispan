package org.infinispan.executors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestException;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Basic tests for {@link LimitedExecutor}
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "executors.LimitedExecutorTest")
public class LimitedExecutorTest extends AbstractInfinispanTest {
   public static final String NAME = "Test";
   private final ExecutorService executor = new ThreadPoolExecutor(1, 1,
         0L, TimeUnit.MILLISECONDS,
         new SynchronousQueue<>(),
         getTestThreadFactory(NAME));

   @AfterClass(alwaysRun = true)
   public void stopExecutors() {
      executor.shutdownNow();
   }

   public void testConcurrency1WithinThread() throws Exception {
      LimitedExecutor limitedExecutor = new LimitedExecutor(NAME, new WithinThreadExecutor(), 1);

      CompletableFuture<String> cf = new CompletableFuture<>();
      limitedExecutor.execute(() -> cf.complete("bla"));
      assertEquals("bla", cf.get());
   }

   public void testConcurrencyLimit() throws Exception {
      LimitedExecutor limitedExecutor = new LimitedExecutor(NAME, executor, 1);
      CountDownLatch latch = new CountDownLatch(1);

      CompletableFuture<String> cf1 = new CompletableFuture<>();
      limitedExecutor.execute(() -> {
         awaitLatch(latch);
         cf1.complete("bla");
      });

      CompletableFuture<String> cf2 = new CompletableFuture<>();
      limitedExecutor.execute(() -> cf2.complete("bla"));

      Thread.sleep(10);
      assertFalse(cf2.isDone());

      latch.countDown();
      assertEquals("bla", cf1.get(10, SECONDS));
      assertEquals("bla", cf2.get(10, SECONDS));
   }

   private void awaitLatch(CountDownLatch latch1) {
      try {
         latch1.await(30, SECONDS);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new TestException(e);
      }
   }

   public void testAsyncTasks() throws Exception {
      LimitedExecutor limitedExecutor = new LimitedExecutor(NAME, executor, 1);

      CompletableFuture<Void> blockingFuture = new CompletableFuture<>();
      CompletableFuture<String> cf1 = new CompletableFuture<>();
      limitedExecutor.executeAsync(() -> blockingFuture.thenRunAsync(() -> cf1.complete("bla")));

      CompletableFuture<String> cf2 = new CompletableFuture<>();
      limitedExecutor.execute(() -> cf2.complete("bla"));

      Thread.sleep(10);
      assertFalse(cf2.isDone());

      blockingFuture.complete(null);
      assertEquals("bla", cf1.get(10, SECONDS));
      assertEquals("bla", cf2.get(10, SECONDS));
   }
}
