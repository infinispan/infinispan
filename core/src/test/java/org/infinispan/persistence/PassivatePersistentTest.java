package org.infinispan.persistence;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.cache.impl.EncoderCache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.CacheContainer;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(testName = "persistence.PassivatePersistentTest", groups = "functional")
public class PassivatePersistentTest extends AbstractInfinispanTest {

   Cache<String, String> cache;
   AdvancedLoadWriteStore store;
   TransactionManager tm;
   ConfigurationBuilder cfg;
   CacheContainer cm;
   StorageType storage;

   @Factory
   public Object[] factory() {
      return new Object[] {
            new PassivatePersistentTest().withStorage(StorageType.BINARY),
            new PassivatePersistentTest().withStorage(StorageType.OBJECT),
            new PassivatePersistentTest().withStorage(StorageType.OFF_HEAP)
      };
   }

   public PassivatePersistentTest withStorage(StorageType storage) {
      this.storage = storage;
      return this;
   }

   @Override
   protected String parameters() {
      return "[storage=" + storage + "]";
   }

   @BeforeMethod
   public void setUp() {
      cfg = new ConfigurationBuilder();
      cfg
         .persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
               .storeName(this.getClass().getName())
               .purgeOnStartup(false)
               .memory().storageType(storage);
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
      assertEquals("v", cache.get("k"));
      cache.evict("k");
      assertTrue(store.contains(getInternalKey("k")));

      assertEquals("v", cache.get("k"));
      assertFalse(store.contains(getInternalKey("k")));

      cache.stop();
      cache.start();
      // The old store's marshaller is not working any more
      store = (AdvancedLoadWriteStore) TestingUtil.getCacheLoader(cache);

      assertTrue(store.contains(getInternalKey("k")));
      assertEquals("v", cache.get("k"));
      assertFalse(store.contains(getInternalKey("k")));
   }

   public Object getInternalKey(String key) {
      if (cache instanceof EncoderCache) {
         return ((EncoderCache) cache).keyToStorage(key);
      } else {
         return key;
      }
   }
}
