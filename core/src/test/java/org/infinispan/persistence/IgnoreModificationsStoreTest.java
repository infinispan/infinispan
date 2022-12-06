package org.infinispan.persistence;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Test read only store,
 * i.e. test proper functionality of setting ignoreModifications(true) for cache store.
 *
 * @author Tomas Sykora
 */
@Test(testName = "persistence.IgnoreModificationsStoreTest", groups = "functional", sequential = true)
@CleanupAfterMethod
public class IgnoreModificationsStoreTest extends SingleCacheManagerTest {
   private static final long EXPIRATION_TIME = 1_000_000;
   DummyInMemoryStore store;
   ControlledTimeService timeService = new ControlledTimeService();

   boolean expiration;

   IgnoreModificationsStoreTest expiration(boolean expiration) {
      this.expiration = expiration;
      return this;
   }

   @Factory
   public Object[] factory() {
      return new Object[]{
            new IgnoreModificationsStoreTest().expiration(false),
            new IgnoreModificationsStoreTest().expiration(true)
      };
   }

   @Override
   protected String parameters() {
      return "[" + expiration + "]";
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(false);
      if (expiration) {
         cfg.expiration().lifespan(EXPIRATION_TIME, TimeUnit.MILLISECONDS);
      }
      cfg
            .persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            // Disable segmentation so we can more easily add/remove entries manually
            .segmented(false)
            // This way we can write to the store still
            .storeName(IgnoreModificationsStoreTest.class.getName())
            .ignoreModifications(true);

      EmbeddedCacheManager ecm = TestCacheManagerFactory.createCacheManager(cfg);
      TestingUtil.replaceComponent(ecm, TimeService.class, timeService, true);
      return ecm;
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      store = TestingUtil.getFirstStore(cache);
   }

   public void testReadOnlyCacheStore() throws PersistenceException, IOException, InterruptedException {
      String storeDataName = IgnoreModificationsStoreTest.class.getName() + "_" + cache.getName();
      Map<Object, byte[]> storeMap = DummyInMemoryStore.getStoreDataForName(storeDataName).get(0);

      DummyInMemoryStore dummyInMemoryStore = (DummyInMemoryStore) TestingUtil.getFirstStoreWait(cache).delegate();
      byte[] storedBytes = dummyInMemoryStore.valueToStoredBytes("v1");
      storeMap.put("k1", storedBytes);

      AssertJUnit.assertEquals("v1", cache.get("k1"));

      TestingUtil.writeToAllStores("k2", "v2", cache);

      AssertJUnit.assertTrue(store.contains("k1"));
      AssertJUnit.assertFalse(store.contains("k2"));

      // put into cache but not into read only store
      cache.put("k2", "v2");
      AssertJUnit.assertEquals("v2", cache.get("k2"));

      AssertJUnit.assertTrue(store.contains("k1"));
      AssertJUnit.assertFalse(store.contains("k2"));

      AssertJUnit.assertFalse(TestingUtil.deleteFromAllStores("k1", cache));
      AssertJUnit.assertFalse(TestingUtil.deleteFromAllStores("k2", cache));
      AssertJUnit.assertFalse(TestingUtil.deleteFromAllStores("k3", cache));

      AssertJUnit.assertEquals("v1", cache.get("k1"));
      AssertJUnit.assertEquals("v2", cache.get("k2"));
      cache.remove("k1");
      cache.remove("k2");
      AssertJUnit.assertNotNull(cache.get("k1"));
      AssertJUnit.assertNull(cache.get("k2"));

      // lastly check what happens if entry is expired but load is called
      if (expiration) {
         dummyInMemoryStore = (DummyInMemoryStore) TestingUtil.getFirstStoreWait(cache).delegate();
         storedBytes = dummyInMemoryStore.valueToStoredBytes("v1-new");
         storeMap.put("k1", storedBytes);

         AssertJUnit.assertEquals("v1", cache.get("k1"));

         timeService.advance(EXPIRATION_TIME + 1);

         AssertJUnit.assertEquals("v1-new", cache.get("k1"));
      }
   }
}
