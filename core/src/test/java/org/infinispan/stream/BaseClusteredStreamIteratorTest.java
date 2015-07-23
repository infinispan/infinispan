package org.infinispan.stream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.filter.CacheFilters;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.KeyFilterAsKeyValueFilter;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Base class for stream iterator tests that are ran on clustered caches
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.BaseClusteredStreamIteratorTest")
public abstract class BaseClusteredStreamIteratorTest extends BaseStreamIteratorTest {
   public BaseClusteredStreamIteratorTest(boolean tx, CacheMode mode) {
      super(tx, mode);
   }

   protected Map<Object, String> putValuesInCache() {
      return putValueInEachCache(3);
   }

   protected Map<Object, String> putValueInEachCache(int cacheNumber) {
      // This is linked to keep insertion order
      Map<Object, String> valuesInserted = new LinkedHashMap<Object, String>();
      for (int i = 0; i < cacheNumber; ++i) {
         Cache<Object, String> cache = cache(i, CACHE_NAME);
         Object key = getKeyTiedToCache(cache);
         cache.put(key, key.toString());
         valuesInserted.put(key, key.toString());
      }
      return valuesInserted;
   }

   @Test
   public void simpleTestIteratorFromOtherNode() {
      Map<Object, String> values = putValuesInCache();

      Cache<MagicKey, String> cache = cache(1, CACHE_NAME);

      Iterator<CacheEntry<MagicKey, String>> iterator = cache.getAdvancedCache().cacheEntrySet().stream().iterator();
      Map<MagicKey, String> results = mapFromIterator(iterator);
      assertEquals(values, results);
   }

   @Test
   public void simpleTestRemoteFilter() {
      Map<Object, String> values = putValuesInCache();
      Iterator<Map.Entry<Object, String>> iter = values.entrySet().iterator();
      Map.Entry<Object, String> excludedEntry = iter.next();
      // Remove it so comparison below will be correct
      iter.remove();

      Cache<MagicKey, String> cache = cache(1, CACHE_NAME);

      Iterator<CacheEntry<MagicKey, String>> iterator = cache.getAdvancedCache().cacheEntrySet().stream().filter(
              CacheFilters.predicate(new KeyFilterAsKeyValueFilter<>(new CollectionKeyFilter<>(
                              Collections.singleton(excludedEntry.getKey()))))).iterator();
      Map<MagicKey, String> results = mapFromIterator(iterator);
      assertEquals(values, results);
   }
}
