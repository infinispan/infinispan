package org.infinispan.iteration;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.KeyFilterAsKeyValueFilter;
import org.infinispan.iteration.impl.EntryRetriever;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

/**
 * Base class for entry retriever tests that are ran on clustered caches
 *
 * @author wburns
 * @since 7.0
 */
public abstract class BaseClusteredEntryRetrieverTest extends BaseEntryRetrieverTest {
   public BaseClusteredEntryRetrieverTest(boolean tx, CacheMode mode) {
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

      EntryRetriever<MagicKey, String> retriever = cache(1, CACHE_NAME).getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      CloseableIterator<CacheEntry<MagicKey, String>> iterator = retriever.retrieveEntries(null, null, null, null);
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


      EntryRetriever<MagicKey, String> retriever = cache(1, CACHE_NAME).getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      CloseableIterator<CacheEntry<MagicKey, String>> iterator = retriever.retrieveEntries(
            new KeyFilterAsKeyValueFilter<MagicKey, String>(new CollectionKeyFilter<>(Collections.singleton(excludedEntry.getKey()))),
            null, null, null);
      Map<MagicKey, String> results = mapFromIterator(iterator);
      assertEquals(values, results);
   }
}
