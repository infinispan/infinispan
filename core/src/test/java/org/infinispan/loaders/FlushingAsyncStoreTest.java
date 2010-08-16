package org.infinispan.loaders;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.math.stat.inference.TestUtils;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.loaders.decorators.AsyncStoreConfig;
import org.infinispan.loaders.file.FileCacheStore;
import org.infinispan.loaders.file.FileCacheStoreConfig;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * FlushingAsyncStoreTest.
 * 
 * @author Sanne Grinovero
 */
@Test(groups = "functional", testName = "loaders.FlushingAsyncStoreTest", sequential = true)
public class FlushingAsyncStoreTest extends SingleCacheManagerTest {

   /** to assert the test methods are run in proper order **/
   private boolean storeWasRun = false;

   public FlushingAsyncStoreTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration config = getDefaultStandaloneConfig(false);
      FileCacheStoreConfig fcsConfig = new FileCacheStoreConfig();
      fcsConfig.setCacheLoaderClassName(SlowFileCacheStore.class.getName());
      AsyncStoreConfig storeConfig = new AsyncStoreConfig();
      storeConfig.setEnabled(true);
      storeConfig.setThreadPoolSize(1);
      fcsConfig.setAsyncStoreConfig(storeConfig);
      CacheLoaderManagerConfig clmConfig = new CacheLoaderManagerConfig();
      clmConfig.getCacheLoaderConfigs().add(fcsConfig);
      config.setCacheLoaderManagerConfig(clmConfig);
      return TestCacheManagerFactory.createCacheManager(config);
   }

   @Test (timeOut = 10000)
   public void writeOnStorage() throws IOException, ClassNotFoundException, SQLException, InterruptedException {
      cache = cacheManager.getCache("AsyncStoreInMemory");
      cache.put("key1", "value");
      cache.stop();
      storeWasRun = true;
   }

   @Test(dependsOnMethods = "writeOnStorage")
   public void verifyStorageContent() throws IOException {
      assert storeWasRun;
      cache = cacheManager.getCache("AsyncStoreInMemory");
      assert "value".equals(cache.get("key1"));
   }
   
   @AfterClass
   public void removeStore(){
      TestUtils a; 
   }

   public static class SlowFileCacheStore extends FileCacheStore {
      private void insertDelay() {
         try {
            Thread.sleep(1000);
         } catch (InterruptedException e) {
         }
      }

      @Override
      protected void insertBucket(Bucket bucket) throws CacheLoaderException {
         insertDelay();
         super.insertBucket(bucket);
      }

      @Override
      protected boolean removeLockSafe(Object key, String lockingKey) throws CacheLoaderException {
         insertDelay();
         return super.removeLockSafe(key, lockingKey);
      }

      @Override
      protected void storeLockSafe(InternalCacheEntry entry, String lockingKey) throws CacheLoaderException {
         insertDelay();
         super.storeLockSafe(entry, lockingKey);
      }
   }
}
