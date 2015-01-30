package org.infinispan.iteration;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.iteration.impl.EntryRetriever;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.CustomClass;
import org.infinispan.metadata.Metadata;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

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
      Map<CustomClass, CustomClass> originalValues = new HashMap<>();
      originalValues.put(new CustomClass("value0"), new CustomClass("cache0"));
      originalValues.put(new CustomClass("value1"), new CustomClass("cache1"));
      originalValues.put(new CustomClass("value2"), new CustomClass("cache2"));

      cache.putAll(originalValues);

      EntryRetriever<CustomClass, CustomClass> retriever = cache.getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      // Try filter for all values
      Iterator<CacheEntry<CustomClass, CustomClass>> iterator = retriever.retrieveEntries(
            new CustomClassFilter(originalValues), null, null, null);

      // we need this count since the map will replace same key'd value
      int count = 0;
      Map<CustomClass, CustomClass> results = new HashMap<>();
      while (iterator.hasNext()) {
         Map.Entry<CustomClass, CustomClass> entry = iterator.next();
         results.put(entry.getKey(), entry.getValue());
         count++;
      }
      assertEquals(3, count);
      assertEquals(originalValues, results);
   }

   @Test
   public void testFilterWithStoreAsBinaryPartialKeys() throws InterruptedException, ExecutionException, TimeoutException {
      Map<CustomClass, CustomClass> originalValues = new HashMap<>();
      originalValues.put(new CustomClass("value0"), new CustomClass("cache0"));
      originalValues.put(new CustomClass("value1"), new CustomClass("cache1"));
      originalValues.put(new CustomClass("value2"), new CustomClass("cache2"));

      cache.putAll(originalValues);

      EntryRetriever<CustomClass, CustomClass> retriever = cache.getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      // Try filter for all values
      Iterator<CacheEntry<CustomClass, CustomClass>> iterator = retriever.retrieveEntries(
            new CustomClassFilter(Collections.singletonMap(new CustomClass("value1"), new CustomClass("cache1"))), null, null, null);

      assertTrue(iterator.hasNext());
      CacheEntry<CustomClass, CustomClass> entry = iterator.next();
      assertEquals(new CustomClass("value1"), entry.getKey());
      assertEquals(new CustomClass("cache1"), entry.getValue());
      assertFalse(iterator.hasNext());
   }

   private static class CustomClassFilter implements KeyValueFilter<CustomClass, CustomClass>, Serializable {
      private final Map<CustomClass, CustomClass> allowedEntries;

      public CustomClassFilter(Map<CustomClass, CustomClass> allowedEntries) {
         this.allowedEntries = allowedEntries;
      }

      @Override
      public boolean accept(CustomClass key, CustomClass value, Metadata metadata) {
         CustomClass allowedValue = allowedEntries.get(key);
         return allowedValue != null && allowedValue.equals(value);
      }
   }
}

