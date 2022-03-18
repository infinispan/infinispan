package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.fail;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.support.WaitDelegatingNonBlockingStore;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.sifs.SoftIndexFileStoreFunctionalTest")
public class SoftIndexFileStoreFunctionalTest extends BaseStoreFunctionalTest {
   protected String tmpDirectory;

   @BeforeClass(alwaysRun = true)
   protected void setUpTempDir() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(getClass());
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence,
         String cacheName, boolean preload) {
      persistence
            .addSoftIndexFileStore()
            .dataLocation(Paths.get(tmpDirectory, "data").toString())
            .indexLocation(Paths.get(tmpDirectory, "index").toString())
            .maxFileSize(1000)
            .purgeOnStartup(false).preload(preload)
            // Effectively disable reaper for tests
            .expiration().wakeUpInterval(Long.MAX_VALUE);
      return persistence;
   }

   // Test for ISPN-13747 - will fail only intermittently, to have it fail every time add a sleep at
   // Index.deleteFileAsync before deleting the actual file when the bug is present
   public void testExpirationThenCompaction() throws Throwable {
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), "testExpirationThenCompaction", false);
      TestingUtil.defineConfiguration(cacheManager, "testExpirationThenCompaction", cb.build());

      ControlledTimeService controlledTimeService = new ControlledTimeService();
      TimeService prev = TestingUtil.replaceComponent(cacheManager, TimeService.class, controlledTimeService, true);

      Cache<String, Object> cache = cacheManager.getCache("testExpirationThenCompaction");
      cache.start();

      try {
         cache.put("k1", "v1", 2, TimeUnit.MILLISECONDS);

         for (int i = 2; i < 22; ++i) {
            cache.put("k" + i, "v" + 2);
         }

         // Our first entry is expired
         controlledTimeService.advance(3);

         WaitDelegatingNonBlockingStore store = TestingUtil.getFirstStoreWait(cache);

         Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");
         Set<Integer> files = compactor.getFiles();
         assertEquals("Test wants 2 files to reproduce reliably", 2, files.size());

         SynchronousQueue<Object> syncQueue = new SynchronousQueue<>();
         // This runs async but will block waiting on queue
         compactor.performExpirationCompaction(new MyCompactionObserver(syncQueue));
         CompletionStage<Void> compactionStage = compactor.forceCompactionForAllNonLogFiles();

         Object result = syncQueue.poll(100, TimeUnit.MINUTES);
         if (result == null) {
            fail("Nothing was received from queue!");
         }

         if (result instanceof Throwable) {
            throw (Throwable) result;
         }
         if (result == syncQueue) {
            fail("No expired entry found!");
         }

         Object complete = syncQueue.poll(100, TimeUnit.MINUTES);
         assertSame("Previous result was: " + result, syncQueue, complete);

         // This makes sure the compactor is still working properly
         compactionStage.toCompletableFuture().get(10, TimeUnit.SECONDS);
      } finally {
         TestingUtil.replaceComponent(cacheManager, TimeService.class, prev, true);
      }
   }

   static class MyCompactionObserver implements Compactor.CompactionExpirationSubscriber {
      private final SynchronousQueue<Object> syncQueue;

      MyCompactionObserver(SynchronousQueue<Object> syncQueue) {
         this.syncQueue = syncQueue;
      }

      @Override
      public void onEntryPosition(EntryPosition entryPosition) throws IOException {
         try {
            syncQueue.offer(entryPosition, 10, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            throw new IOException(e);
         }
      }

      @Override
      public void onEntryEntryRecord(EntryRecord entryRecord) throws IOException {
         try {
            syncQueue.offer(entryRecord, 10, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            throw new IOException(e);
         }
      }

      @Override
      public void onComplete() {
         try {
            syncQueue.offer(syncQueue, 10, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            throw new CacheException(e);
         }
      }

      @Override
      public void onError(Throwable t) {
         log.warn("Throwable encountered: ", t);
         try {
            syncQueue.offer(t, 10, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            throw new CacheException(e);
         }
      }
   }
}
