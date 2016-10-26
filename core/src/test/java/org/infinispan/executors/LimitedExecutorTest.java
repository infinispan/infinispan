package org.infinispan.executors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Basic tests for {@link LimitedExecutor}
 *
 * @author Dan Berindei
 * @since 9.0
 */
@Test(groups = "functional", testName = "executors.LimitedExecutorTest")
public class LimitedExecutorTest extends AbstractInfinispanTest {
   public static final String NAME = "Test";
   private final ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2,
         0L, MILLISECONDS,
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

   /**
    * Test that no more than 1 task runs at a time.
    */
   public void testConcurrencyLimit() throws Exception {
      eventuallyEquals(0, executor::getActiveCount);
      LimitedExecutor limitedExecutor = new LimitedExecutor(NAME, executor, 1);
      CompletableFuture<String> blocker1 = new CompletableFuture<>();
      CompletableFuture<String> blocker2 = new CompletableFuture<>();

      CompletableFuture<String> cf1 = new CompletableFuture<>();
      limitedExecutor.execute(() -> {
         try {
            cf1.complete(blocker1.get(10, SECONDS));
         } catch (Exception e) {
            cf1.completeExceptionally(e);
         }
      });

      CompletableFuture<String> cf2 = new CompletableFuture<>();
      limitedExecutor.execute(() -> {
         try {
            cf2.complete(cf1.getNow("task 2 ran too early") + " " + blocker2.get(10, SECONDS));
         } catch (Exception e) {
            cf2.completeExceptionally(e);
         }
      });

      assertFalse(cf1.isDone());
      assertFalse(cf2.isDone());

      blocker1.complete("value1");
      blocker2.complete("value2");
      assertEquals("value1", cf1.get(10, SECONDS));
      assertEquals("value1 value2", cf2.get(10, SECONDS));
      eventuallyEquals(0, executor::getActiveCount);
   }

   /**
    * Test that an async task ({@code executeAsync()}) will block another task from running
    * until its {@code CompletableFuture} is completed.
    */
   public void testAsyncTasks() throws Exception {
      eventuallyEquals(0, executor::getActiveCount);
      LimitedExecutor limitedExecutor = new LimitedExecutor(NAME, executor, 1);
      CompletableFuture<String> blocker1 = new CompletableFuture<>();
      CompletableFuture<String> blocker2 = new CompletableFuture<>();

      CompletableFuture<String> cf1 = new CompletableFuture<>();
      limitedExecutor.executeAsync(() -> blocker1.thenAccept(cf1::complete));

      CompletableFuture<String> cf2 = new CompletableFuture<>();
      limitedExecutor.execute(() -> {
         try {
            cf2.complete(cf1.getNow("task 2 ran too early") + " " + blocker2.get(10, SECONDS));
         } catch (Exception e) {
            cf2.completeExceptionally(e);
         }
      });

      assertFalse(cf1.isDone());
      assertFalse(cf2.isDone());

      blocker1.complete("value1");
      blocker2.complete("value2");
      assertEquals("value1", cf1.get(10, SECONDS));
      assertEquals("value1 value2", cf2.get(10, SECONDS));
      eventuallyEquals(0, executor::getActiveCount);
   }
}
