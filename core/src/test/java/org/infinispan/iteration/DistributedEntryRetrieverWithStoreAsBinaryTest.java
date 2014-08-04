package org.infinispan.iteration;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.iteration.impl.EntryRetriever;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

/**
 * Test to verify distributed entry behavior when store as binary is used
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "distexec.DistributedEntryRetrieverWithStoreAsBinaryTest")
public class DistributedEntryRetrieverWithStoreAsBinaryTest extends MultipleCacheManagersTest {
   protected final static String CACHE_NAME = "DistributedEntryRetrieverWithStoreAsBinaryTest";
   protected ConfigurationBuilder builderUsed;
   protected final boolean tx = false;
   protected final CacheMode cacheMode = CacheMode.DIST_SYNC;

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      builderUsed.clustering().hash().numOwners(1);
      builderUsed.dataContainer().storeAsBinary().enabled(true).storeKeysAsBinary(true).storeValuesAsBinary(true);
      if (tx) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      createClusteredCaches(3, CACHE_NAME, builderUsed);
   }

   @Test
   public void testFilterWithStoreAsBinary() throws InterruptedException, ExecutionException, TimeoutException {
      Cache<MagicKey, String> cache0 = cache(0, CACHE_NAME);
      Cache<MagicKey, String> cache1 = cache(1, CACHE_NAME);
      Cache<MagicKey, String> cache2 = cache(2, CACHE_NAME);

      Map<MagicKey, String> originalValues = new HashMap<>();
      originalValues.put(new MagicKey(cache0), "cache0");
      originalValues.put(new MagicKey(cache1), "cache1");
      originalValues.put(new MagicKey(cache2), "cache2");

      cache0.putAll(originalValues);

      EntryRetriever<MagicKey, String> retriever = cache1.getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      // Try filter for all values
      Iterator<CacheEntry<MagicKey, String>> iterator = retriever.retrieveEntries(
            new MagicKeyStringFilter(originalValues), null, null, null);

      // we need this count since the map will replace same key'd value
      int count = 0;
      Map<MagicKey, String> results = new HashMap<MagicKey, String>();
      while (iterator.hasNext()) {
         Map.Entry<MagicKey, String> entry = iterator.next();
         results.put(entry.getKey(), entry.getValue());
         count++;
      }
      assertEquals(count, 3);
      assertEquals(originalValues, results);
   }

   @Test
   public void testFilterWithStoreAsBinaryPartialKeys() throws InterruptedException, ExecutionException, TimeoutException {
      Cache<MagicKey, String> cache0 = cache(0, CACHE_NAME);
      Cache<MagicKey, String> cache1 = cache(1, CACHE_NAME);
      Cache<MagicKey, String> cache2 = cache(2, CACHE_NAME);

      MagicKey findKey = new MagicKey(cache1);
      Map<MagicKey, String> originalValues = new HashMap<>();
      originalValues.put(new MagicKey(cache0), "cache0");
      originalValues.put(findKey, "cache1");
      originalValues.put(new MagicKey(cache2), "cache2");

      cache0.putAll(originalValues);

      EntryRetriever<MagicKey, String> retriever = cache2.getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      // Try filter for all values
      Iterator<CacheEntry<MagicKey, String>> iterator = retriever.retrieveEntries(
            new MagicKeyStringFilter(Collections.singletonMap(findKey, "cache1")), null, null, null);

      CacheEntry<MagicKey, String> entry = iterator.next();
      AssertJUnit.assertEquals(findKey, entry.getKey());
      AssertJUnit.assertEquals("cache1", entry.getValue());
      assertFalse(iterator.hasNext());
   }

   private static class MagicKeyStringFilter implements KeyValueFilter<MagicKey, String>, Serializable {
      private final Map<MagicKey, String> allowedEntries;

      public MagicKeyStringFilter(Map<MagicKey, String> allowedEntries) {
         this.allowedEntries = allowedEntries;
      }

      @Override
      public boolean accept(MagicKey key, String value, Metadata metadata) {
         String allowedValue = allowedEntries.get(key);
         return allowedValue != null && allowedValue.equals(value);
      }
   }
}

