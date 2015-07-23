package org.infinispan.stream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.filter.CacheFilters;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.CompositeKeyValueFilterConverter;
import org.infinispan.filter.KeyFilterAsKeyValueFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * Base class for stream iterator tests
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.BaseStreamIteratorTest")
public abstract class BaseStreamIteratorTest extends BaseSetupStreamIteratorTest {
   public BaseStreamIteratorTest(boolean tx, CacheMode mode) {
      super(tx, mode);
   }

   protected abstract Object getKeyTiedToCache(Cache<?, ?> cache);

   protected Map<Object, String> putValuesInCache() {
      // This is linked to keep insertion order
      Map<Object, String> valuesInserted = new LinkedHashMap<Object, String>();
      Cache<Object, String> cache = cache(0, CACHE_NAME);
      Object key = getKeyTiedToCache(cache);
      cache.put(key, key.toString());
      valuesInserted.put(key, key.toString());
      return valuesInserted;
   }

   @Test
   public void simpleTest() {
      Map<Object, String> values = putValuesInCache();

      Cache<MagicKey, String> cache = cache(0, CACHE_NAME);
      Iterator<Map.Entry<MagicKey, String>> iterator = cache.entrySet().iterator();
      Map<MagicKey, String> results = mapFromIterator(iterator);
      assertEquals(values, results);
   }

   @Test
   public void simpleTestIteratorWithMetadata() {
      // This is linked to keep insertion order
      Set<CacheEntry<Object, String>> valuesInserted = new HashSet<>();
      Cache<Object, String> cache = cache(0, CACHE_NAME);
      for (int i = 0; i < 3; ++i) {
         Object key = getKeyTiedToCache(cache);
         TimeUnit unit = TimeUnit.MINUTES;
         cache.put(key, key.toString(), 10, unit, i + 1, unit);

         valuesInserted.add(new TransientMortalCacheEntry(key, key.toString(), unit.toMillis(i + 1), unit.toMillis(10),
                                                          System.currentTimeMillis()));
      }

      Set<CacheEntry<Object, String>> retrievedValues = new HashSet<>();
      Iterator<CacheEntry<Object, String>> iterator = cache.getAdvancedCache().cacheEntrySet().stream().iterator();
      while (iterator.hasNext()) {
         CacheEntry<Object, String> entry = iterator.next();
         retrievedValues.add(entry);
      }
      assertEquals(retrievedValues.size(), valuesInserted.size());
      // Have to do our own equals since Transient uses created time which we can't guarantee will equal
      for (CacheEntry<Object, String> inserted : valuesInserted) {
         CacheEntry<Object, String> found = null;
         for (CacheEntry<Object, String> retrieved : retrievedValues) {
            if (retrieved.getKey().equals(inserted.getKey())) {
               found = retrieved;
               break;
            }
         }
         assertNotNull("No retrieved Value matching" + inserted, found);
         assertEquals(found.getValue(), inserted.getValue());
         assertEquals(found.getMaxIdle(), inserted.getMaxIdle());
         assertEquals(found.getLifespan(), inserted.getLifespan());
      }
   }

   @Test
   public void simpleTestLocalFilter() {
      Map<Object, String> values = putValuesInCache();
      Iterator<Map.Entry<Object, String>> iter = values.entrySet().iterator();
      Map.Entry<Object, String> excludedEntry = iter.next();
      // Remove it so comparison below will be correct
      iter.remove();

      Cache<MagicKey, String> cache = cache(0, CACHE_NAME);

      KeyValueFilter<MagicKey, String> filter = new KeyFilterAsKeyValueFilter<>(new CollectionKeyFilter<>(
              Collections.singleton(excludedEntry.getKey())));
      Iterator<CacheEntry<MagicKey, String>> iterator = cache.getAdvancedCache().cacheEntrySet().stream().filter(
              CacheFilters.predicate(filter)).iterator();
      Map<MagicKey, String> results = mapFromIterator(iterator);
      assertEquals(values, results);
   }

   @Test
   public void testFilterAndConverterCombined() {
      Map<Object, String> values = putValuesInCache();
      Iterator<Map.Entry<Object, String>> iter = values.entrySet().iterator();
      Map.Entry<Object, String> excludedEntry = iter.next();
      // Remove it so comparison below will be correct
      iter.remove();


      Cache<MagicKey, String> cache = cache(0, CACHE_NAME);
      KeyValueFilterConverter<MagicKey, String, String> filterConverter = new CompositeKeyValueFilterConverter<>(
            new KeyFilterAsKeyValueFilter<>(new CollectionKeyFilter<>(Collections.singleton(excludedEntry.getKey()))),
            new StringTruncator(2, 5));
      try (Stream<CacheEntry<MagicKey, String>> stream = CacheFilters.filterAndConvert(
              cache.getAdvancedCache().cacheEntrySet().stream(), filterConverter)) {
         Map<MagicKey, String> results = mapFromStream(stream);

         assertEquals(values.size(), results.size());
         for (Map.Entry<Object, String> entry : values.entrySet()) {
            assertEquals(entry.getValue().substring(2, 7), results.get(entry.getKey()));
         }
      }
   }
}

