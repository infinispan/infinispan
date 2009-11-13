package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "statetransfer.StateTransferCacheLoaderFunctionalTest", enabled = true)
public class StateTransferCacheLoaderFunctionalTest extends StateTransferFunctionalTest {
   int id;
   ThreadLocal<Boolean> sharedCacheLoader = new ThreadLocal<Boolean>() {
      protected Boolean initialValue() {
         return false;
      }
   };

   public StateTransferCacheLoaderFunctionalTest() {
      cacheName = "nbst-with-loader";
   }

   @Override
   protected CacheManager createCacheManager() {
      // increment the DIMCS store id
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      CacheStoreConfig clc = new DummyInMemoryCacheStore.Cfg("store number " + id++);
      clmc.addCacheLoaderConfig(clc);
      clc.setFetchPersistentState(true);
      clmc.setShared(sharedCacheLoader.get());
      config.setCacheLoaderManagerConfig(clmc);
      return super.createCacheManager();
   }

   @Override
   protected void writeInitialData(final Cache<Object, Object> c) {
      super.writeInitialData(c);
      c.evict(A_B_NAME);
      c.evict(A_B_AGE);
      c.evict(A_C_NAME);
      c.evict(A_C_AGE);
      c.evict(A_D_NAME);
      c.evict(A_D_AGE);
   }

   protected void verifyInitialDataOnLoader(Cache<Object, Object> c) throws Exception {
      CacheLoader l = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheLoader();
      assert l.containsKey(A_B_AGE);
      assert l.containsKey(A_B_NAME);
      assert l.containsKey(A_C_AGE);
      assert l.containsKey(A_C_NAME);
      assert l.load(A_B_AGE).getValue().equals(TWENTY);
      assert l.load(A_B_NAME).getValue().equals(JOE);
      assert l.load(A_C_AGE).getValue().equals(FORTY);
      assert l.load(A_C_NAME).getValue().equals(BOB);
   }

   protected void verifyNoData(Cache<Object, Object> c) {
      assert c.isEmpty() : "Cache should be empty!";
   }

   protected void verifyNoDataOnLoader(Cache<Object, Object> c) throws Exception {
      CacheLoader l = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheLoader();
      assert !l.containsKey(A_B_AGE);
      assert !l.containsKey(A_B_NAME);
      assert !l.containsKey(A_C_AGE);
      assert !l.containsKey(A_C_NAME);
      assert !l.containsKey(A_D_AGE);
      assert !l.containsKey(A_D_NAME);
   }

   public void testSharedLoader() throws Exception {
      try {
         sharedCacheLoader.set(true);
         Cache<Object, Object> c1 = createCacheManager().getCache(cacheName);
         writeInitialData(c1);

         // starting the second cache would initialize an in-memory state transfer but not a persistent one since the loader is shared
         Cache<Object, Object> c2 = createCacheManager().getCache(cacheName);

         TestingUtil.blockUntilViewsReceived(60000, c1, c2);

         verifyInitialDataOnLoader(c1);
         verifyInitialData(c1);

         verifyNoDataOnLoader(c2);
         verifyNoData(c2);
      } finally {
         sharedCacheLoader.set(false);
      }
   }
}
