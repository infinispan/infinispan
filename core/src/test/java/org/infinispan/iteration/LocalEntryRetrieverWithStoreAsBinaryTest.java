package org.infinispan.iteration;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.iteration.impl.EntryRetriever;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

/**
 * Test to verify local entry behavior when store as binary is used
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "distexec.LocalEntryRetrieverWithStoreAsBinaryTest")
public class LocalEntryRetrieverWithStoreAsBinaryTest extends SingleCacheManagerTest {
   protected final static String CACHE_NAME = "LocalEntryRetrieverWithStoreAsBinaryTest";
   protected ConfigurationBuilder builderUsed;
   protected final boolean tx = false;
   protected final CacheMode cacheMode = CacheMode.DIST_SYNC;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      builderUsed = new ConfigurationBuilder();
      builderUsed.dataContainer().storeAsBinary().enabled(true).storeKeysAsBinary(true).storeValuesAsBinary(true);
      if (tx) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      return TestCacheManagerFactory.createCacheManager(builderUsed);
   }

   @Test
   public void testFilterWithStoreAsBinary() throws InterruptedException, ExecutionException, TimeoutException {
      Map<String, String> originalValues = new HashMap<>();
      originalValues.put("value0", "cache0");
      originalValues.put("value1", "cache1");
      originalValues.put("value2", "cache2");

      cache.putAll(originalValues);

      EntryRetriever<String, String> retriever = cache.getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      // Try filter for all values
      Iterator<CacheEntry<String, String>> iterator = retriever.retrieveEntries(
            new StringStringFilter(originalValues), null, null, null);

      // we need this count since the map will replace same key'd value
      int count = 0;
      Map<String, String> results = new HashMap<>();
      while (iterator.hasNext()) {
         Map.Entry<String, String> entry = iterator.next();
         results.put(entry.getKey(), entry.getValue());
         count++;
      }
      assertEquals(3, count);
      assertEquals(originalValues, results);
   }

   @Test
   public void testFilterWithStoreAsBinaryPartialKeys() throws InterruptedException, ExecutionException, TimeoutException {
      Map<String, String> originalValues = new HashMap<>();
      originalValues.put("value0", "cache0");
      originalValues.put("value1", "cache1");
      originalValues.put("value2", "cache2");

      cache.putAll(originalValues);

      EntryRetriever<String, String> retriever = cache.getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      // Try filter for all values
      Iterator<CacheEntry<String, String>> iterator = retriever.retrieveEntries(
            new StringStringFilter(Collections.singletonMap("value1", "cache1")), null, null, null);

      CacheEntry<String, String> entry = iterator.next();
      assertEquals("value1", entry.getKey());
      assertEquals("cache1", entry.getValue());
      assertFalse(iterator.hasNext());
   }

   private static class StringStringFilter implements KeyValueFilter<String, String>, Serializable {
      private final Map<String, String> allowedEntries;

      public StringStringFilter(Map<String, String> allowedEntries) {
         this.allowedEntries = allowedEntries;
      }

      @Override
      public boolean accept(String key, String value, Metadata metadata) {
         String allowedValue = allowedEntries.get(key);
         return allowedValue != null && allowedValue.equals(value);
      }
   }
}

