package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.loaders.manager.CacheLoaderManager;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

@Test(testName = "loaders.PassivatePersistentTest", groups = "functional")
public class PassivatePersistentTest extends AbstractInfinispanTest {

   Cache<String, String> cache;
   CacheStore store;
   TransactionManager tm;
   ConfigurationBuilder cfg;
   CacheContainer cm;

   @BeforeMethod
   public void setUp() {
      cfg = new ConfigurationBuilder();
      cfg
         .loaders()
            .passivation(true)
            .addStore(DummyInMemoryCacheStoreConfigurationBuilder.class)
               .storeName(this.getClass().getName())
               .purgeOnStartup(false);
      cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
      tm = TestingUtil.getTransactionManager(cache);
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      store.clear();
      TestingUtil.killCacheManagers(cm);
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
      // The old store's marshaller is not working any more
      store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();

      assert store.containsKey("k");
      assert "v".equals(cache.get("k"));
      assert !store.containsKey("k");
   }
}
