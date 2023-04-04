package org.infinispan.stream;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import jakarta.transaction.NotSupportedException;
import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.filter.CacheFilters;
import org.infinispan.filter.CompositeKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.testng.annotations.Test;

/**
 * Test to verify distributed stream iterator when in a tx
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = {"functional", "smoke"}, testName = "stream.DistributedStreamIteratorRepeatableReadTxTest")
public class DistributedStreamIteratorRepeatableReadTxTest extends DistributedStreamIteratorTest {
   public DistributedStreamIteratorRepeatableReadTxTest() {
      super(true, CacheMode.DIST_SYNC);
   }

   /**
    * See ISPN-12731 Cache collections ignore values added in transaction
    */
   public void testCacheCollectionsIncludesEntriesNotYetCommitted() throws Exception {
      Cache<Object, String> cache = cache(0, CACHE_NAME);
      TransactionManager tm = tm(cache);
      tm.begin();
      try {
         Map<Object, String> inserted = new LinkedHashMap<>();
         for (int i = 0; i < 3; ++i) {
            Object key = new MagicKey(cache(i, CACHE_NAME));
            cache.put(key, key.toString());
            inserted.put(key, key.toString());
         }

         // cache collections use streams internally
         Set<Object> expectedKeys = inserted.keySet();
         Set<Object> keySetResults = new HashSet<>(cache.keySet());
         assertEquals(expectedKeys, keySetResults);

         Set<String> expectedValues = new HashSet<>(inserted.values());
         Set<String> valuesResults = new HashSet<>(cache.values());
         assertEquals(expectedValues, valuesResults);

         Set<Map.Entry<Object, String>> expectedEntries = inserted.entrySet();
         Set<Map.Entry<Object, String>> entrySetResults = new HashSet<>(cache.entrySet());
         assertEquals(expectedEntries, entrySetResults);
      } finally {
         tm.rollback();
      }
   }

   public void testFilterWithExistingTransaction() throws Exception {
      Map<Object, String> values = putValueInEachCache(3);

      Cache<Object, String> cache = cache(0, CACHE_NAME);
      TransactionManager tm = tm(cache);
      tm.begin();
      try {
         Object key = "filtered-key";
         cache.put(key, "filtered-value");

         Iterator<CacheEntry<Object, String>> iterator = cache.getAdvancedCache().cacheEntrySet().stream()
               .filter(entry -> !Objects.equals(key, entry.getKey()))
               .iterator();
         Map<Object, String> results = mapFromIterator(iterator);
         assertEquals(values, results);
      } finally {
         tm.rollback();
      }
   }

   @Test
   public void testConverterWithExistingTransaction() throws NotSupportedException, SystemException {
      Map<Object, String> values = putValuesInCache();

      Cache<Object, String> cache = cache(0, CACHE_NAME);
      TransactionManager tm = tm(cache);
      tm.begin();
      try {
         Object key = "converted-key";
         String value = "converted-value";
         values.put(key, value);
         cache.put(key, "converted-value");

         try (CacheStream<CacheEntry<Object, String>> stream = cache.getAdvancedCache().cacheEntrySet().stream().
                 filter(CacheFilters.predicate(AcceptAllKeyValueFilter.getInstance())).
                 map(CacheFilters.function(new StringTruncator(2, 5)))) {
            Map<Object, String> results = mapFromStream(stream);

            assertEquals(values.size(), results.size());
            for (Map.Entry<Object, String> entry : values.entrySet()) {
               assertEquals(entry.getValue().substring(2, 7), results.get(entry.getKey()));
            }
         }
      } finally {
         tm.rollback();
      }
   }

   @Test
   public void testKeyFilterConverterWithExistingTransaction() throws NotSupportedException, SystemException {
      Map<Object, String> values = putValuesInCache();

      Cache<Object, String> cache = cache(0, CACHE_NAME);
      TransactionManager tm = tm(cache);
      tm.begin();
      try {
         Iterator<Map.Entry<Object, String>> iter = values.entrySet().iterator();
         Map.Entry<Object, String> extraEntry = iter.next();
         while (iter.hasNext()) {
            iter.next();
            iter.remove();
         }

         Object key = "converted-key";
         String value = "converted-value";
         values.put(key, value);
         cache.put(key, "converted-value");

         Collection<Object> acceptedKeys = new ArrayList<>();
         acceptedKeys.add(key);
         acceptedKeys.add(extraEntry.getKey());

         KeyValueFilter<Object, String> filter = (Serializable & KeyValueFilter<Object, String>)(k, v, m) -> acceptedKeys.contains(k);
         KeyValueFilterConverter<Object, String, String> filterConverter = new CompositeKeyValueFilterConverter<>(filter,
               new StringTruncator(2, 5));
         try (CacheStream<CacheEntry<Object, String>> stream = CacheFilters.filterAndConvert(
                 cache.getAdvancedCache().cacheEntrySet().stream(), filterConverter)) {
            Map<Object, String> results = mapFromStream(stream);
            assertEquals(values.size(), results.size());
            for (Map.Entry<Object, String> entry : values.entrySet()) {
               assertEquals(entry.getValue().substring(2, 7), results.get(entry.getKey()));
            }
         }
      } finally {
         tm.rollback();
      }
   }

   public void testStreamWithMissedKeyInTransaction() throws Exception {
      AdvancedCache<Object, String> cache = advancedCache(0, CACHE_NAME);
      TransactionManager tm = tm(cache);

      tm.begin();
      try {
         Object localMissingKey = new MagicKey("key1", cache);
         Object remoteMissingKey = new MagicKey("key2", cache(1, CACHE_NAME));

         assertFalse(cache.containsKey(localMissingKey));
         assertFalse(cache.containsKey(remoteMissingKey));
         Iterator<CacheEntry<Object, String>> iterator = cache.getAdvancedCache().cacheEntrySet().stream().iterator();
         Map<Object, String> results = mapFromIterator(iterator);
         assertEquals(Collections.emptyMap(), results);
         // size() also uses streams internally
         assertEquals(0, cache.size());
      } finally {
         tm.rollback();
      }
   }
}
