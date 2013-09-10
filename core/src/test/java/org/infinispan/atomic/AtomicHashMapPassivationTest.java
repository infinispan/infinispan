package org.infinispan.atomic;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.marshall.core.MarshalledEntry;
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

   CacheLoader loader;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(true);
      c
         .invocationBatching().enable()
         .persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(c);
      cache = cm.getCache();
      loader = TestingUtil.getFirstLoader(cache);
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

      log.trace("About to evict...");
      cache.evict(key);
      assertInStoreNotInCache(key);

      tm.begin();
      map = AtomicMapLookup.getAtomicMap(cache, key);
      map.put("a", "c");
      map.put("d", "e");
      tm.commit();
   }

   private void assertInCacheNotInStore(Object key) throws CacheLoaderException {
      InternalCacheValue ice = cache.getAdvancedCache().getDataContainer().get(key).toInternalCacheValue();
      testStoredEntry(ice, key, "Cache");
      assert !loader.contains(key) : "Key " + key + " should not be in store!";
   }

   private void testStoredEntry(InternalCacheValue entry, Object key, String src) {
      assert entry != null : src + " entry for key " + key + " should NOT be null";
   }

   private void testStoredEntry(MarshalledEntry entry, Object key, String src) {
      assert entry != null : src + " entry for key " + key + " should NOT be null";
   }

   private void assertInStoreNotInCache(Object key) throws CacheLoaderException {
      MarshalledEntry se = loader.load(key);
      testStoredEntry(se, key, "Store");
      assert !cache.getAdvancedCache().getDataContainer().containsKey(key) : "Key " + key + " should not be in cache!";
   }


}
