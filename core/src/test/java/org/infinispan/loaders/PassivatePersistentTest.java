package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

@Test(testName = "loaders.PassivatePersistentTest", groups = "functional")
public class PassivatePersistentTest extends AbstractInfinispanTest {

   Cache<String, String> cache;
   CacheStore store;
   TransactionManager tm;
   Configuration cfg;
   CacheContainer cm;

   @BeforeTest
   public void setUp() {
      cfg = new Configuration();
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      clmc.setPassivation(true);
      clmc.addCacheLoaderConfig(new DummyInMemoryCacheStore.Cfg(false));
      cfg.setCacheLoaderManagerConfig(clmc);
      cm = TestCacheManagerFactory.createCacheManager(cfg, true);
      cache = cm.getCache();
      store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
      tm = TestingUtil.getTransactionManager(cache);
   }

   @AfterTest
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
   }

   @AfterMethod
   public void afterMethod() throws CacheLoaderException {
      if (cache != null) cache.clear();
      if (store != null) store.clear();
   }

   public void testPersistence() throws CacheLoaderException {
      cache.put("k", "v");
      assert "v".equals(cache.get("k"));
      cache.evict("k");
      assert store.containsKey("k");

      assert "v".equals(cache.get("k"));
      assert !store.containsKey("k");

      cache.stop();
      cache.start();

      assert store.containsKey("k");
      assert "v".equals(cache.get("k"));
      assert !store.containsKey("k");
   }
}
