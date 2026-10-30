package org.infinispan.manager;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.commons.ThreadGroups;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Verifies that work submitted to the local node from a blocking thread does not run in the submitting thread.
 * <p>
 * A caller on a blocking thread may hold something the submitted work needs, for example, a cache start waiting for a
 * reindex that needs the started cache. Running the work inline in that thread deadlocks. The submissions run on a platform
 * thread in {@link ThreadGroups#BLOCKING_GROUP}, so the test does not depend on whether the blocking thread pool uses
 * virtual threads.
 * </p>
 */
@Test(groups = "functional", testName = "manager.ClusterExecutorBlockingCallerTest")
public class ClusterExecutorBlockingCallerTest extends AbstractInfinispanTest {

   private static final long TIMEOUT_SECONDS = 10;

   private EmbeddedCacheManager cacheManager;

   @AfterMethod(alwaysRun = true)
   public void tearDown() {
      TestingUtil.killCacheManagers(cacheManager);
      cacheManager = null;
   }

   public void testLocalSubmitConsumerFromBlockingThread() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager();
      submitFromBlockingThreadAndRelease(release -> cacheManager.executor()
            .submitConsumer(ignore -> awaitRelease(release), (address, value, t) -> {}));
   }

   public void testClusteredSubmitConsumerFromBlockingThread() throws Exception {
      // A single member, so the only target is the local node.
      cacheManager = TestCacheManagerFactory.createClusteredCacheManager();
      submitFromBlockingThreadAndRelease(release -> cacheManager.executor()
            .submitConsumer(ignore -> awaitRelease(release), (address, value, t) -> {}));
   }

   public void testLocalSubmitRunnableFromBlockingThread() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager();
      submitFromBlockingThreadAndRelease(release -> cacheManager.executor()
            .submit(() -> awaitRelease(release)));
   }

   public void testLocalExecuteFromBlockingThread() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager();
      submitFromBlockingThreadAndRelease(release -> {
         // execute() has no result, so the command signals its own completion.
         CompletableFuture<Void> done = new CompletableFuture<>();
         cacheManager.executor().execute(() -> {
            awaitRelease(release);
            done.complete(null);
         });
         return done;
      });
   }

   /**
    * Submits work from a blocking thread and releases the work only after the submission returns.
    * <p>
    * If the work runs inline in the submitting thread, the submission never returns and the test fails with a
    * {@link java.util.concurrent.TimeoutException}.
    *
    * @param submission submits work that waits for the given latch and returns the future of that work.
    */
   private static void submitFromBlockingThreadAndRelease(Function<CountDownLatch, CompletableFuture<?>> submission) throws Exception {
      CountDownLatch release = new CountDownLatch(1);
      try {
         CompletableFuture<?> work = callFromBlockingThread(() -> submission.apply(release));

         release.countDown();
         work.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } finally {
         // Unblocks the work in case the submission timed out.
         release.countDown();
      }
   }

   /**
    * Runs the given action on a new platform thread that belongs to the blocking thread group.
    *
    * @return the value returned by the action, waiting at most {@link #TIMEOUT_SECONDS}.
    */
   private static <T> T callFromBlockingThread(Callable<T> action) throws Exception {
      FutureTask<T> task = new FutureTask<>(action);
      Thread thread = new Thread(ThreadGroups.BLOCKING_GROUP, task, "ClusterExecutorBlockingCallerTest-blocking");
      thread.setDaemon(true);
      thread.start();
      return task.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
   }

   private static Void awaitRelease(CountDownLatch release) {
      try {
         // Bounded, so a failed run does not leave the thread waiting forever.
         // Waits longer than the submission timeout, so an inline run fails on the submission and not here.
         assertTrue(release.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS), "Work was never released");
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new RuntimeException(e);
      }
      return null;
   }
}
