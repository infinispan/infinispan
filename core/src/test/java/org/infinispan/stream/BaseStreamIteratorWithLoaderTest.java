package org.infinispan.stream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.testng.Assert.assertEquals;

/**
 * Base test to verify stream behavior when a loader is present
 *
 * @author afield
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.BaseStreamIteratorWithLoaderTest")
public abstract class BaseStreamIteratorWithLoaderTest extends MultipleCacheManagersTest {
   protected String cacheName = "BaseStreamIteratorWithLoaderTest";
   protected ConfigurationBuilder builderUsed;
   protected final boolean tx;
   protected final CacheMode cacheMode;

   public BaseStreamIteratorWithLoaderTest(boolean tx, CacheMode cacheMode, String cacheName) {
      this.tx = tx;
      this.cacheMode = cacheMode;
      this.cacheName = cacheName;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      builderUsed.clustering().hash().numOwners(1);
      builderUsed.persistence().passivation(false).addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(cacheName);
      if (tx) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      if (cacheMode.isClustered()) {
         createClusteredCaches(3, cacheName, builderUsed);
      } else {
         createClusteredCaches(1, cacheName, builderUsed);
      }
   }

   private Map<Object, String> insertDefaultValues(boolean includeLoaderEntry) {
      Cache<Object, String> cache0 = cache(0, cacheName);
      Map<Object, String> originalValues = new HashMap<>();
      Object loaderKey;
      if (cacheMode.isDistributed() || cacheMode.isReplicated()) {
         Cache<Object, String> cache1 = cache(1, cacheName);
         Cache<Object, String> cache2 = cache(2, cacheName);
         originalValues.put(new MagicKey(cache0), "cache0");
         originalValues.put(new MagicKey(cache1), "cache1");
         originalValues.put(new MagicKey(cache2), "cache2");
         loaderKey = new MagicKey(cache2);
      } else {
         originalValues.put(1, "value0");
         originalValues.put(2, "value1");
         originalValues.put(3, "value2");
         loaderKey = 4;
      }

      cache0.putAll(originalValues);

      PersistenceManager persistenceManager = TestingUtil.extractComponent(cache0, PersistenceManager.class);
      DummyInMemoryStore store = persistenceManager.getStores(DummyInMemoryStore.class).iterator().next();

      TestObjectStreamMarshaller sm = new TestObjectStreamMarshaller();
      try {
         String loaderValue = "loader-value";
         store.write(new MarshalledEntryImpl(loaderKey, loaderValue, null, sm));
         if (includeLoaderEntry) {
            originalValues.put(loaderKey, loaderValue);
         }
      } finally {
         sm.stop();
      }
      return originalValues;
   }

   @Test
   public void testCacheLoader() throws InterruptedException, ExecutionException, TimeoutException {
      Map<Object, String> originalValues = insertDefaultValues(true);
      Cache<Object, String> cache = cache(0, cacheName);

      Iterator<Map.Entry<Object, String>> iterator = cache.entrySet().stream().iterator();

      // we need this count since the map will replace same key'd value
      int count = 0;
      Map<Object, String> results = new HashMap<Object, String>();
      while (iterator.hasNext()) {
         Map.Entry<Object, String> entry = iterator.next();
         results.put(entry.getKey(), entry.getValue());
         count++;
      }
      assertEquals(count, 4);
      assertEquals(originalValues, results);
   }

   @Test
   public void testCacheLoaderIgnored() throws InterruptedException, ExecutionException, TimeoutException {
      Map<Object, String> originalValues = insertDefaultValues(false);
      Cache<Object, String> cache = cache(0, cacheName);

      Iterator<Map.Entry<Object, String>> iterator = cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).
              entrySet().stream().iterator();

      // we need this count since the map will replace same key'd value
      int count = 0;
      Map<Object, String> results = new HashMap<>();
      while (iterator.hasNext()) {
         Map.Entry<Object, String> entry = iterator.next();
         results.put(entry.getKey(), entry.getValue());
         count++;
      }
      assertEquals(count, 3);
      assertEquals(originalValues, results);
   }
}
