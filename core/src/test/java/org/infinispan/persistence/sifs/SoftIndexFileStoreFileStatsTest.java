package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.support.WaitDelegatingNonBlockingStore;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.BlockingManagerTestUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.sifs.SoftIndexFileStoreFileStatsTest")
public class SoftIndexFileStoreFileStatsTest extends SingleCacheManagerTest {
   protected String tmpDirectory;

   @BeforeClass(alwaysRun = true)
   protected void setUpTempDir() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(getClass());
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
      global.globalState().persistentLocation(CommonsTestingUtil.tmpDirectory(this.getClass()));
      return TestCacheManagerFactory.newDefaultCacheManager(true, global, new ConfigurationBuilder());
   }

   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence) {
      persistence
            .addSoftIndexFileStore()
            .dataLocation(Paths.get(tmpDirectory, "data").toString())
            .indexLocation(Paths.get(tmpDirectory, "index").toString())
            .maxFileSize(1000)
            .purgeOnStartup(false)
            // Effectively disable reaper for tests
            .expiration().wakeUpInterval(Long.MAX_VALUE);
      return persistence;
   }

   void configureCache(String cacheName) {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      createCacheStoreConfig(cb.persistence());
      TestingUtil.defineConfiguration(cacheManager, cacheName, cb.build());
   }

   ControlledTimeService defineCacheConfigurationAndInjectTimeService(String cacheName) {
      configureCache(cacheName);

      ControlledTimeService controlledTimeService = new ControlledTimeService();
      TimeService prev = TestingUtil.replaceComponent(cacheManager, TimeService.class, controlledTimeService, true);
      controlledTimeService.setActualTimeService(prev);

      return controlledTimeService;
   }

   static class MyCompactionObserver implements Compactor.CompactionExpirationSubscriber {
      private final BlockingQueue<Object> syncQueue;
      private final CountDownLatch completionLatch = new CountDownLatch(1);
      private volatile Throwable error;

      MyCompactionObserver(BlockingQueue<Object> syncQueue) {
         this.syncQueue = syncQueue;
      }

      @Override
      public void onEntryPosition(EntryPosition entryPosition) throws IOException {
         try {
            log.trace("EntryPosition found: " + entryPosition);
            if (!syncQueue.offer(entryPosition, 10, TimeUnit.SECONDS)) {
               fail("Queue did not accept offer of an EntryPosition!");
            }
         } catch (InterruptedException e) {
            throw new IOException(e);
         }
      }

      @Override
      public void onEntryEntryRecord(EntryRecord entryRecord) throws IOException {
         try {
            log.trace("EntryRecord found: " + entryRecord);
            if (!syncQueue.offer(entryRecord, 10, TimeUnit.SECONDS)) {
               fail("Queue did not accept offer of an EntryRecord!");
            }
         } catch (InterruptedException e) {
            throw new IOException(e);
         }
      }

      @Override
      public void onComplete() {
         log.trace("Expiration compaction completed");
         completionLatch.countDown();
      }

      @Override
      public void onError(Throwable t) {
         log.warn("Throwable encountered: ", t);
         error = t;
         completionLatch.countDown();
      }

      void waitForCompletion() throws InterruptedException {
         assertTrue(completionLatch.await(10, TimeUnit.SECONDS));
         if (error != null) {
            throw new AssertionError(error);
         }
      }
   }

   Map.Entry<Integer, Compactor.Stats> extractCompletedStat(ConcurrentMap<Integer, Compactor.Stats> statsMap) {
      return statsMap.entrySet().stream().filter(e -> e.getValue().isCompleted()).findFirst()
            .orElse(null);
   }

   @Test(dataProvider = "booleans")
   public void testOverwriteLogFileSize(Method m, boolean performExpirationAfterFirst) throws InterruptedException {
      String cacheName = m.getName() + "-" + performExpirationAfterFirst;
      configureCache(cacheName);

      BlockingManagerTestUtil.replaceManagersWithInline(cacheManager);

      Cache<String, Object> cache = cacheManager.getCache(cacheName);
      cache.start();

      WaitDelegatingNonBlockingStore store = TestingUtil.getFirstStoreWait(cache);

      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");

      ConcurrentMap<Integer, Compactor.Stats> statsMap = compactor.getFileStats();
      cache.put("k1", "v1");

      cache.put("k1", "v1");

      if (performExpirationAfterFirst) {
         MyCompactionObserver myCompactionObserver = new MyCompactionObserver(new ArrayBlockingQueue<>(5));
         compactor.performExpirationCompaction(myCompactionObserver);
         myCompactionObserver.waitForCompletion();
      }
      assertEquals(1, statsMap.size());

      Map.Entry<Integer, Compactor.Stats> entry = statsMap.entrySet().iterator().next();

      int maxInserts = 100;
      int insertions = 0;
      while (statsMap.containsKey(entry.getKey())) {
         cache.put("k1", "v1");
         if (++insertions == maxInserts) {
            fail("Failed to remove stats map after " + maxInserts + " stats were: " + statsMap);
         }
      }
   }

   @DataProvider(name = "booleans")
   public static Object[][] booleans() {
      return new Object[][]{{Boolean.FALSE}, {Boolean.TRUE}};
   }

   @Test(dataProvider = "booleans")
   public void testExpirationStats(Method m, boolean extraRemovedEntry) throws InterruptedException {
      String cacheName = m.getName() + "-" + extraRemovedEntry;
      ControlledTimeService controlledTimeService = defineCacheConfigurationAndInjectTimeService(cacheName);
      BlockingManagerTestUtil.replaceManagersWithInline(cacheManager);
      try {
         Cache<String, Object> cache = cacheManager.getCache(cacheName);
         cache.start();

         WaitDelegatingNonBlockingStore store = TestingUtil.getFirstStoreWait(cache);

         Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");

         ConcurrentMap<Integer, Compactor.Stats> statsMap = compactor.getFileStats();
         cache.put("k1", "v1", 3, TimeUnit.MILLISECONDS);
         long expectedExpirationTime = controlledTimeService.wallClockTime() + 3;

         assertEquals(0, statsMap.size());

         // This causes the logFile to be added to the statsMap before expiration compaction
         if (extraRemovedEntry) {
            cache.put("removed", "remove-me");
            cache.remove("removed");
            assertEquals(1, statsMap.size());

            Compactor.Stats stat = statsMap.values().iterator().next();
            int freeAmount = stat.getFree();

            assertTrue("Something should have been freed", freeAmount != 0);
         }

         controlledTimeService.advance(4);
         BlockingQueue<Object> queue = new ArrayBlockingQueue<>(10);

         MyCompactionObserver myCompactionObserver = new MyCompactionObserver(queue);
         compactor.performExpirationCompaction(myCompactionObserver);

         // Due to inline BlockingManager will be done on same thread so already finished
         myCompactionObserver.waitForCompletion();
         assertEquals(1, queue.size());

         assertEquals(1, statsMap.size());

         Compactor.Stats stat = statsMap.values().iterator().next();
         int freeAmount = stat.getFree();

         assertTrue("Something should have been freed", freeAmount != 0);

         int i = 1;
         // Fill it up until we have 1 complete logFile, 1 compacted file and 1 logFile
         Map.Entry<Integer, Compactor.Stats> entry;
         while ((entry = extractCompletedStat(statsMap)) == null) {
            cache.put("k" + i, "v" + i, 4, TimeUnit.MILLISECONDS);
            i++;
            if (i > 100) {
               fail("Shouldn't require 100 iterations...");
            }
         }

         Compactor.Stats completedStats = entry.getValue();

         assertTrue("Stats were: " + completedStats, completedStats.getFree() > 0);
         assertEquals(expectedExpirationTime, completedStats.getNextExpirationTime());
         assertFalse(completedStats.isScheduled());

         // Now we remove the other values until the file gets compacted
         for (int j = 1; j < i; ++j) {
            cache.remove("k" + j);
            if (!statsMap.containsKey(entry.getKey())) {
               completedStats = null;
               break;
            }
         }

         assertNull("File " + entry.getKey() + " was still not removed... stats were: " + statsMap, completedStats);
      } finally {
         TestingUtil.replaceComponent(cacheManager, TimeService.class, controlledTimeService.getActualTimeService(), true);
      }
   }

   public void testExpirationCompactionOnLogFile(Method m) throws InterruptedException {
      String cacheName = m.getName();

      ControlledTimeService controlledTimeService = defineCacheConfigurationAndInjectTimeService(cacheName);
      try {
         Cache<String, Object> cache = cacheManager.getCache(cacheName);
         cache.start();

         cache.put("expired", "bar", 10, TimeUnit.SECONDS);

         // This value will be overwritten in a loop later to cause log file to roll over
         cache.put("replace-me", "1");

         controlledTimeService.advance(TimeUnit.SECONDS.toMillis(11));

         NonBlockingSoftIndexFileStore store = TestingUtil.getFirstStore(cache);

         Compactor compactor = TestingUtil.extractField(store, "compactor");

         // We use a sync queue so we can block the compaction and insert values
         SynchronousQueue<Object> queue = new SynchronousQueue<>();

         MyCompactionObserver myCompactionObserver = new MyCompactionObserver(queue);
         // This will be blocked by the expired entry above
         compactor.performExpirationCompaction(myCompactionObserver);

         int increment = 2;
         ConcurrentMap<Integer, Compactor.Stats> fileStats = compactor.getFileStats();
         while (extractCompletedStat(fileStats) == null) {
            cache.put("replace-me", String.valueOf(increment++));
         }

         // Let compaction expiration complete now
         assertNotNull(queue.poll(10, TimeUnit.SECONDS));

         // Compactor should clean up the old file and it should just be a single entry for the removed entry
         eventually(() -> "File stats are: " + fileStats + " and data directory size is: " +
                     SoftIndexFileStoreTestUtils.dataDirectorySize(tmpDirectory, cacheName)
               , () -> SoftIndexFileStoreTestUtils.dataDirectorySize(tmpDirectory, cacheName) < 1000L);
      } finally {
         TestingUtil.replaceComponent(cacheManager, TimeService.class, controlledTimeService.getActualTimeService(), true);
      }
   }
}
