package org.infinispan.lucene.readlocks;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileMetadata;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;
import org.testng.AssertJUnit;

/**
 * Stress test for {@link org.infinispan.lucene.readlocks.LocalLockMergingSegmentReadLocker}. See also ISPN-4497 for an
 * example of race conditions it protects against.
 *
 * @author Sanne Grinovero
 * @since 7.0
 */
@Test(groups = "profiling", testName = "lucene.readlocks.LocalLockStressTest")
public class LocalLockStressTest extends SingleCacheManagerTest {

   static final int NUM_THREADS = 10;
   static final int TEST_MINUTES_MAX = 10;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder configurationBuilder = CacheTestSupport.createLocalCacheConfiguration();
      return TestCacheManagerFactory.createCacheManager(configurationBuilder);
   }

   @Test
   public void testMultiThreaded() {
      final Cache<Object, Object> metadata = cacheManager.getCache("metadata");
      final Cache<Object, Object> chunks = cacheManager.getCache("chunks");
      final Cache<Object, Integer> locks = cacheManager.getCache("locks");

      FileMetadata fileMetadata = new FileMetadata(10);
      fileMetadata.setSize(11); // Make it chunked otherwise no read lock will involved
      metadata.put(new FileCacheKey("indexName", "fileName"), fileMetadata);
      final LocalLockMergingSegmentReadLocker locker = new LocalLockMergingSegmentReadLocker(locks, chunks, metadata, "indexName");
      final AtomicBoolean testFailed = new AtomicBoolean(false);
      final ExecutorService exec = Executors.newFixedThreadPool(NUM_THREADS);

      Runnable stressor = new Runnable() {
         @Override
         public void run() {
            try {
               int counter = 0;
               while (testFailed.get() == false) {

                  locker.acquireReadLock("fileName");
                  Thread.sleep(2);
                  locker.deleteOrReleaseReadLock("fileName");

                  // Take a break every now and a again to try and avoid the same LocalReadLock being used constantly
                  if (counter++ % 900 == 0) {
                     System.out.print(".");
                     Thread.sleep(7);
                  }
                  if (metadata.get(new FileCacheKey("indexName", "fileName")) == null) {
                     //Shouldn't have been deleted!
                     testFailed.set(true);
                     System.out.print("X");
                  }
               }
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               return;
            }
         }
      };
      for (int i = 0; i < NUM_THREADS; i++) {
         exec.execute(stressor);
      }
      System.out.println("Stressor threads started...");
      exec.shutdown();
      try {
         exec.awaitTermination(TEST_MINUTES_MAX, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
         exec.shutdownNow();
      }
      AssertJUnit.assertFalse(testFailed.get());
   }

}
