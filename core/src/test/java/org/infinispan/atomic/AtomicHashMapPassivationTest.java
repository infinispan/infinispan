package org.infinispan.atomic;

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.lang.reflect.Method;

/**
 * Tests passivation of atomic hash map instances.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "atomic.AtomicHashMapPassivationTest")
public class AtomicHashMapPassivationTest extends SingleCacheManagerTest {

   CacheStore store;
   
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration c = getDefaultStandaloneConfig(true);
      c.setInvocationBatchingEnabled(true);
      c.setUseLazyDeserialization(true);
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      clmc.setPassivation(true);
      clmc.addCacheLoaderConfig(new DummyInMemoryCacheStore.Cfg());
      c.setCacheLoaderManagerConfig(clmc);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(c, true);
      cache = cm.getCache();
      store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
      return cm;
   }

   public void testPassivateAndUpdate(Method m) throws Exception {
      String key = "k-" + m.getName();
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      tm.begin();
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, key);
      assert map.isEmpty();
      map.put("a", "b");
      assert map.get("a").equals("b");
      tm.commit();
      assertInCacheNotInStore(key);

      cache.evict(key);
      assertInStoreNotInCache(key);

      tm.begin();
      map = AtomicMapLookup.getAtomicMap(cache, key);
      map.put("a", "c");
      map.put("d", "e");
      tm.commit();
   }

   private void assertInCacheNotInStore(Object key) throws CacheLoaderException {
      InternalCacheEntry se = cache.getAdvancedCache().getDataContainer().get(key);
      testStoredEntry(se, key, "Cache");
      assert !store.containsKey(key) : "Key " + key + " should not be in store!";
   }

   private void testStoredEntry(InternalCacheEntry entry, Object key, String src) {
      assert entry != null : src + " entry for key " + key + " should NOT be null";
   }

   private void assertInStoreNotInCache(Object key) throws CacheLoaderException {
      InternalCacheEntry se = store.load(key);
      testStoredEntry(se, key, "Store");
      assert !cache.getAdvancedCache().getDataContainer().containsKey(key) : "Key " + key + " should not be in cache!";
   }

   
}
