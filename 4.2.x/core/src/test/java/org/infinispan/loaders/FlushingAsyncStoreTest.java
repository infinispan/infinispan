package org.infinispan.loaders;

import java.io.IOException;
import java.sql.SQLException;

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.decorators.AsyncStoreConfig;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
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
      CacheStoreConfig slowCacheStoreConfig = new SlowCacheStoreConfig();
      AsyncStoreConfig storeConfig = new AsyncStoreConfig();
      storeConfig.setEnabled(true);
      storeConfig.setThreadPoolSize(1);
      slowCacheStoreConfig.setAsyncStoreConfig(storeConfig);
      CacheLoaderManagerConfig clmConfig = new CacheLoaderManagerConfig();
      clmConfig.getCacheLoaderConfigs().add(slowCacheStoreConfig);
      config.setCacheLoaderManagerConfig(clmConfig);
      return TestCacheManagerFactory.createCacheManager(config);
   }

   @Test(timeOut = 10000)
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
   
   public static class SlowCacheStoreConfig extends DummyInMemoryCacheStore.Cfg {
      public SlowCacheStoreConfig() {
         setCacheLoaderClassName(SlowCacheStore.class.getName());
      }
   }

   public static class SlowCacheStore extends DummyInMemoryCacheStore {
      private void insertDelay() {
         TestingUtil.sleepThread(100);
      }

      @Override
      public void store(InternalCacheEntry ed) {
         insertDelay();
         super.store(ed);
      }
   }
}
