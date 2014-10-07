package org.infinispan.iteration;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.CompositeKeyValueFilterConverter;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyFilterAsKeyValueFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.iteration.impl.EntryRetriever;
import org.infinispan.metadata.Metadata;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Base class for entry retriever tests
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "iteration.BaseEntryRetrieverTest")
public abstract class BaseEntryRetrieverTest extends BaseSetupEntryRetrieverTest {
   public BaseEntryRetrieverTest(boolean tx, CacheMode mode) {
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

      EntryRetriever<MagicKey, String> retriever = cache(0, CACHE_NAME).getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      try (CloseableIterator<CacheEntry<MagicKey, String>> iterator = retriever.retrieveEntries(null, null, null, null)) {
         Map<MagicKey, String> results = mapFromIterator(iterator);
         assertEquals(values, results);
      }
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

      EntryRetriever<Object, String> retriever = cache(0, CACHE_NAME).getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);
      Set<CacheEntry<Object, String>> retrievedValues = new HashSet<>();
      try (CloseableIterator<CacheEntry<Object, String>> iterator = retriever.retrieveEntries(null, null, null, null)) {
         while (iterator.hasNext()) {
            CacheEntry<Object, String> entry = iterator.next();
            retrievedValues.add(entry);
         }
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
         assertNotNull(found, "No retrieved Value matching" + inserted);
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

      EntryRetriever<MagicKey, String> retriever = cache(0, CACHE_NAME).getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      KeyValueFilter<MagicKey, String> filter = new KeyFilterAsKeyValueFilter<>(new CollectionKeyFilter<>(Collections.singleton(excludedEntry.getKey())));
      try (CloseableIterator<CacheEntry<MagicKey, String>> iterator = retriever.retrieveEntries(filter, null, null, null)) {
         Map<MagicKey, String> results = mapFromIterator(iterator);
         assertEquals(values, results);
      }
   }

   @Test
   public void testPublicAPI() {
      Map<Object, String> values = putValuesInCache();
      Iterator<Map.Entry<Object, String>> iter = values.entrySet().iterator();
      Map.Entry<Object, String> excludedEntry = iter.next();
      // Remove it so comparison below will be correct
      iter.remove();


      Cache<MagicKey, String> cache = cache(0, CACHE_NAME);
      KeyValueFilter<MagicKey, String> filter = new KeyFilterAsKeyValueFilter<>(new CollectionKeyFilter<>(Collections.singleton(excludedEntry.getKey())));
      try (EntryIterable<MagicKey, String> iterable = cache.getAdvancedCache().filterEntries(filter)) {
         Map<MagicKey, String> results = mapFromIterable(iterable);
         assertEquals(values, results);
      }
   }

   @Test
   public void testPublicAPIWithConverter() {
      Map<Object, String> values = putValuesInCache();
      Iterator<Map.Entry<Object, String>> iter = values.entrySet().iterator();
      Map.Entry<Object, String> excludedEntry = iter.next();
      // Remove it so comparison below will be correct
      iter.remove();


      Cache<MagicKey, String> cache = cache(0, CACHE_NAME);
      KeyValueFilter<MagicKey, String> filter = new KeyFilterAsKeyValueFilter<>(new CollectionKeyFilter<>(Collections.singleton(excludedEntry.getKey())));
      try (EntryIterable<MagicKey, String> iterable = cache.getAdvancedCache().filterEntries(filter)) {
         Map<MagicKey, String> results = mapFromIterable(iterable.converter(new StringTruncator(2, 5)));

         assertEquals(values.size(), results.size());
         for (Map.Entry<Object, String> entry : values.entrySet()) {
            assertEquals(entry.getValue().substring(2, 7), results.get(entry.getKey()));
         }
      }
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
      try (EntryIterable<MagicKey, String> iterable = cache.getAdvancedCache().filterEntries(filterConverter)) {
         Map<MagicKey, String> results = mapFromIterable(iterable);

         assertEquals(values.size(), results.size());
         for (Map.Entry<Object, String> entry : values.entrySet()) {
            assertEquals(entry.getValue().substring(2, 7), results.get(entry.getKey()));
         }
      }
   }

   protected static class StringTruncator implements Converter<Object, String, String>, Serializable {
      private final int beginning;
      private final int length;

      public StringTruncator(int beginning, int length) {
         this.beginning = beginning;
         this.length = length;
      }

      @Override
      public String convert(Object key, String value, Metadata metadata) {
         if (value != null && value.length() > beginning + length) {
            return value.substring(beginning, beginning + length);
         } else {
            return value;
         }
      }
   }
}

