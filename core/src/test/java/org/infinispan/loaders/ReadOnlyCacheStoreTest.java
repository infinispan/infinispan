package org.infinispan.loaders;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.loaders.manager.CacheLoaderManager;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.Test;

/**
 * Test read only store,
 * i.e. test proper functionality of setting ignoreModifications(true) for cache store.
 *
 * @author Tomas Sykora
 */
@Test(testName = "loaders.ReadOnlyCacheStoreTest", groups = "functional", sequential = true)
@CleanupAfterMethod
public class ReadOnlyCacheStoreTest extends SingleCacheManagerTest {
   CacheStore store;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .invocationBatching().enable()
         .loaders()
            .addStore(DummyInMemoryCacheStoreConfigurationBuilder.class)
               .ignoreModifications(true);

      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
   }

   public void testReadOnlyCacheStore() throws CacheLoaderException {
      // ignore modifications
      store.store(TestInternalCacheEntryFactory.create("k1", "v1"));
      store.store(TestInternalCacheEntryFactory.create("k2", "v2"));

      assert !store.containsKey("k1") : "READ ONLY - Store should NOT contain k1 key.";
      assert !store.containsKey("k2") : "READ ONLY - Store should NOT contain k2 key.";

      // put into cache but not into read only store
      cache.put("k1", "v1");
      cache.put("k2", "v2");
      assert "v1".equals(cache.get("k1"));
      assert "v2".equals(cache.get("k2"));

      assert !store.containsKey("k1") : "READ ONLY - Store should NOT contain k1 key.";
      assert !store.containsKey("k2") : "READ ONLY - Store should NOT contain k2 key.";

      assert !store.remove("k1") : "READ ONLY - Remove operation should return false (no op)";
      assert !store.remove("k2") : "READ ONLY - Remove operation should return false (no op)";
      assert !store.remove("k3") : "READ ONLY - Remove operation should return false (no op)";

      assert "v1".equals(cache.get("k1"));
      assert "v2".equals(cache.get("k2"));
      cache.remove("k1");
      cache.remove("k2");
      assert cache.get("k1") == null;
      assert cache.get("k2") == null;
   }
}
