package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.Testing;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Tests for SoftIndexFileStore segment removal and compaction scenarios
 */
@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreSegmentRemovalTest")
public class SoftIndexFileStoreSegmentRemovalTest extends SingleCacheManagerTest {
   private static final Log log = Log.getLog(SoftIndexFileStoreSegmentRemovalTest.class);

   private String tmpDirectory;

   @BeforeClass(alwaysRun = true)
   @Override
   protected void createBeforeClass() throws Throwable {
      tmpDirectory = Testing.tmpDirectory(getClass());
      super.createBeforeClass();
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroyAfterClass() {
      super.destroyAfterClass();
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.globalState().enable().persistentLocation(Testing.tmpDirectory(this.getClass()));

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().hash().numSegments(2);
      builder.persistence()
            .addSoftIndexFileStore()
            .dataLocation(Paths.get(tmpDirectory, "data").toString())
            .indexLocation(Paths.get(tmpDirectory, "index").toString())
            .maxFileSize(1000);

      return TestCacheManagerFactory.newDefaultCacheManager(true, global, builder);
   }

   /**
    * Tests concurrent execution of removeSegments and compaction.
    * This is a regression test for scenarios where:
    * 1. addSegments is called to initialize segments
    * 2. Data is written to a segment
    * 3. removeSegments is invoked, which asynchronously cleans old index information
    * 4. While that cleanup is blocked/delayed, compaction is triggered
    * 5. After compaction completes, the cleanup proceeds
    * 6. Both operations complete successfully without corruption
    */
   public void testRemoveSegmentsDuringCompaction() throws Exception {
      Cache<String, String> cache = cacheManager.getCache();

      // Write enough values into segment 0 to force the log appender to open another file
      // maxFileSize is 1000 bytes, so we need to write enough data to exceed that
      // Use larger values with unique content to prevent early compaction
      StringBuilder largeValue = new StringBuilder(200);
      for (int j = 0; j < 200; j++) {
         largeValue.append('x');
      }

      for (int i = 0; i < 20; i++) {
         String key = "key-" + i;
         // Add unique nanoTime to prevent compaction from deduplicating
         String value = largeValue + "-" + i + "-" + System.nanoTime();
         cache.put(key, value);
      }

      NonBlockingSoftIndexFileStore store = TestingUtil.getFirstStore(cache);
      Compactor compactor = TestingUtil.extractField(store, "compactor");
      Index index = TestingUtil.extractField(store, "index");

      // Verify that multiple files were created
      int fileCount = compactor.getFiles().size();
      assertTrue("Should have multiple files for compaction, but had " + fileCount, fileCount > 1);

      CountDownLatch cleanupStarted = new CountDownLatch(1);
      CountDownLatch compactionComplete = new CountDownLatch(1);
      AtomicBoolean firstTaskSeen = new AtomicBoolean(false);

      // Get the original executor
      Executor originalExecutor = TestingUtil.extractField(index, "executor");

      // Create a delaying executor that blocks the first task
      Executor delayingExecutor = command -> {
         if (firstTaskSeen.compareAndSet(false, true)) {
            // This is the first task (from removeSegments)
            originalExecutor.execute(() -> {
               // Signal that cleanup has started
               log.trace("Delaying executor: first task started, signaling latch");
               cleanupStarted.countDown();
               // Wait for compaction to complete
               try {
                  log.trace("Delaying executor: waiting for compaction to complete...");
                  boolean completed = compactionComplete.await(10, TimeUnit.SECONDS);
                  log.tracef("Delaying executor: compaction complete signal received: %s", completed);
                  assertTrue("Compaction should complete before cleanup proceeds", completed);
               } catch (InterruptedException e) {
                  throw new RuntimeException(e);
               }
               // Now execute the actual task
               command.run();
            });
         } else {
            // All subsequent tasks run normally
            originalExecutor.execute(command);
         }
      };

      // Inject the delaying executor
      TestingUtil.replaceField(delayingExecutor, "executor", index, Index.class);

      try {
         // Start removeSegments in background - the executor invokes the cleanup part
         store.removeSegments(IntSets.immutableSet(0)).toCompletableFuture().get(10, TimeUnit.SECONDS);

         // Wait for cleanup to start
         assertTrue("Cleanup should have started", cleanupStarted.await(10, TimeUnit.SECONDS));

         // Now invoke compaction while the segment cleanup is blocked
         try {
            log.trace("Starting compaction...");
            compactor.forceCompactionForAllNonLogFiles().toCompletableFuture().get(10, TimeUnit.SECONDS);
            log.trace("Compaction completed successfully");
         } catch (Exception e) {
            log.tracef(e, "Compaction failed: %s", e.getMessage());
         } finally {
            // Signal compaction is complete (even if it failed)
            compactionComplete.countDown();
            log.trace("Signaled compaction complete");
         }

         // Extract and wait for the removeSegmentsStage to complete
         // This will throw any exception that occurred during the cleanup task
         CompletionStage<Void> removeSegmentsStage = TestingUtil.extractField(index, "removeSegmentsStage");
         removeSegmentsStage.toCompletableFuture().get(10, TimeUnit.SECONDS);

         // Verify file stats are the same after removeSegments
         Map<Integer, Compactor.Stats> fileStats = compactor.getFileStats();
         // Note that total of -1 is alright, that means it is a non completed file
         if (fileStats.values().stream().anyMatch(s -> s.getTotal() == 0)) {
            fail("At least one stat shows no total: " + fileStats);
         }

         // Verify the store is still in a consistent state - entries should be removed
         assertNull(TestingUtil.join(store.load(0, "key-0")));
         assertNull(TestingUtil.join(store.load(0, "key-10")));
         assertNull(TestingUtil.join(store.load(0, "key-19")));

         // Verify that iteration doesn't return any of the removed values
         AtomicInteger iterationCount = new AtomicInteger(0);
         Flowable.fromPublisher(
               store.publishEntries(IntSets.immutableSet(0), null, true))
               .blockingForEach(entry -> iterationCount.incrementAndGet(), 128);
         assertEquals("Iteration should not return any entries after segment 0 removal", 0, iterationCount.get());
      } finally {
         // Restore the original executor
         TestingUtil.replaceField(originalExecutor, "executor", index, Index.class);
      }
   }
}
