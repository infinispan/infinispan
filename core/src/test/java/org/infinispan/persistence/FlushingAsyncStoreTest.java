package org.infinispan.persistence;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
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
      return TestCacheManagerFactory.createCacheManager(config);
   }

   @Test(timeOut = 10000)
   public void writeOnStorage() {
      TestResourceTracker.backgroundTestStarted(this);
      cache = cacheManager.getCache("AsyncStoreInMemory");
      cache.put("key1", "value");
      cache.stop();
      storeWasRun = true;
   }

   @Test(dependsOnMethods = "writeOnStorage")
   public void verifyStorageContent() {
      assert storeWasRun;
      cache = cacheManager.getCache("AsyncStoreInMemory");
      assert "value".equals(cache.get("key1"));
   }
}
