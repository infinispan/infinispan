package org.infinispan.persistence;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.manager.CacheContainer;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

@Test(testName = "persistence.PassivatePersistentTest", groups = "functional")
public class PassivatePersistentTest extends AbstractInfinispanTest {

   Cache<String, String> cache;
   AdvancedLoadWriteStore store;
   TransactionManager tm;
   ConfigurationBuilder cfg;
   CacheContainer cm;

   @BeforeMethod
   public void setUp() {
      cfg = new ConfigurationBuilder();
      cfg
         .persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
               .storeName(this.getClass().getName())
               .purgeOnStartup(false);
      cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      store = (AdvancedLoadWriteStore) TestingUtil.getCacheLoader(cache);
      tm = TestingUtil.getTransactionManager(cache);
   }

   @AfterMethod
   public void tearDown() throws PersistenceException {
      store.clear();
      TestingUtil.killCacheManagers(cm);
   }

   public void testPersistence() throws PersistenceException {
      cache.put("k", "v");
      assert "v".equals(cache.get("k"));
      cache.evict("k");
      assert store.contains("k");

      assert "v".equals(cache.get("k"));
      assert !store.contains("k");

      cache.stop();
      cache.start();
      // The old store's marshaller is not working any more
      store = (AdvancedLoadWriteStore) TestingUtil.getCacheLoader(cache);

      assert store.contains("k");
      assert "v".equals(cache.get("k"));
      assert !store.contains("k");
   }
}
