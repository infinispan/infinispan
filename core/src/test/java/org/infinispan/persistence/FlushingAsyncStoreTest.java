package org.infinispan.persistence;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestResourceTracker;
import org.testng.annotations.Test;

/**
 * FlushingAsyncStoreTest.
 *
 * @author Sanne Grinovero
 */
@Test(groups = "functional", testName = "persistence.FlushingAsyncStoreTest", singleThreaded = true)
public class FlushingAsyncStoreTest extends SingleCacheManagerTest {

   public static final String CACHE_NAME = "AsyncStoreInMemory";
   /** to assert the test methods are run in proper order **/
   private boolean storeWasRun = false;

   public FlushingAsyncStoreTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = getDefaultStandaloneCacheConfig(false);
      config
         .persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
               .storeName(this.getClass().getName())
               .slow(true)
               .async().enable().threadPoolSize(1)
         .build();
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(config);
      cacheManager.defineConfiguration(CACHE_NAME, config.build());
      return cacheManager;
   }

   @Test(timeOut = 10000)
   public void writeOnStorage() {
      TestResourceTracker.testThreadStarted(this);
      cache = cacheManager.getCache(CACHE_NAME);
      cache.put("key1", "value");
      cache.stop();
      storeWasRun = true;
   }

   @Test(dependsOnMethods = "writeOnStorage")
   public void verifyStorageContent() {
      assert storeWasRun;
      cache = cacheManager.getCache(CACHE_NAME);
      assert "value".equals(cache.get("key1"));
   }
}
