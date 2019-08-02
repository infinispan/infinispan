package org.infinispan.client.hotrod.impl.iteration;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.filter.ParamKeyValueFilterConverterFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 8.0
 */
@Test(groups = "functional")
public abstract class BaseMultiServerRemoteIteratorTest extends MultiHotRodServersTest implements AbstractRemoteIteratorTest {

   public static final int CACHE_SIZE = 20;

   @BeforeMethod
   public void clear() {
      clients.forEach(c -> c.getCache().clear());
   }

   @Test
   public void testBatchSizes() {
      int maximumBatchSize = 120;
      RemoteCache<Integer, AccountHS> cache = clients.get(0).getCache();

      populateCache(CACHE_SIZE, this::newAccount, cache);
      Set<Integer> expectedKeys = rangeAsSet(0, CACHE_SIZE);

      for (int batch = 1; batch < maximumBatchSize; batch += 10) {
         Set<Entry<Object, Object>> results = new HashSet<>(CACHE_SIZE);
         CloseableIterator<Entry<Object, Object>> iterator = cache.retrieveEntries(null, null, batch);
         iterator.forEachRemaining(results::add);
         iterator.close();
         assertEquals(CACHE_SIZE, results.size());
         assertEquals(expectedKeys, extractKeys(results));
      }
   }

   @Test
   public void testEmptyCache() {
      try (CloseableIterator<Entry<Object, Object>> iterator = client(0).getCache().retrieveEntries(null, null, 100)) {
         assertFalse(iterator.hasNext());
         assertFalse(iterator.hasNext());
      }
   }

   @Test
   public void testFilterBySegmentAndCustomFilter() {
      String toHexConverterName = "toHexConverter";
      servers.forEach(s -> s.addKeyValueFilterConverterFactory(toHexConverterName, new ToHexConverterFactory()));

      RemoteCache<Integer, Integer> numbersCache = clients.get(0).getCache();
      populateCache(CACHE_SIZE, i -> i, numbersCache);

      Set<Integer> segments = setOf(15, 20, 25);
      Set<Entry<Object, Object>> entries = new HashSet<>();
      try (CloseableIterator<Entry<Object, Object>> iterator = numbersCache.retrieveEntries(toHexConverterName, segments, 10)) {
         while (iterator.hasNext()) {
            entries.add(iterator.next());
         }
      }

      Set<String> values = extractValues(entries);
      getKeysFromSegments(segments).forEach(i -> assertTrue(values.contains(Integer.toHexString(i))));
   }

   @Test
   public void testFilterByCustomParamFilter() {
      String factoryName = "substringConverter";
      servers.forEach(s -> s.addKeyValueFilterConverterFactory(factoryName, new SubstringFilterFactory()));
      int filterParam = 12;

      RemoteCache<String, String> stringCache = clients.get(0).getCache();
      IntStream.rangeClosed(0, CACHE_SIZE - 1).forEach(idx -> stringCache.put(String.valueOf(idx), UUID.randomUUID().toString()));

      Set<Entry<Object, Object>> entries = extractEntries(stringCache.retrieveEntries(factoryName, new Object[]{filterParam}, null, 10));

      Set<String> values = extractValues(entries);
      assertForAll(values, s -> s.length() == filterParam);

      // Omitting param, filter should use default value
      entries = extractEntries(stringCache.retrieveEntries(factoryName, 10));
      values = extractValues(entries);
      assertForAll(values, s -> s.length() == SubstringFilterFactory.DEFAULT_LENGTH);
   }


   @Test
   public void testFilterBySegment() {
      RemoteCache<Integer, AccountHS> cache = clients.get(0).getCache();
      populateCache(CACHE_SIZE, this::newAccount, cache);

      Set<Integer> filterBySegments = rangeAsSet(30, 40);

      Set<Entry<Object, Object>> entries = new HashSet<>();
      try (CloseableIterator<Entry<Object, Object>> iterator = cache.retrieveEntries(null, filterBySegments, 10)) {
         while (iterator.hasNext()) {
            entries.add(iterator.next());
         }
      }

      Marshaller marshaller = clients.get(0).getMarshaller();
      KeyPartitioner keyPartitioner = TestingUtil.extractComponent(cache(0), KeyPartitioner.class);

      assertKeysInSegment(entries, filterBySegments, marshaller, keyPartitioner::getSegment);
   }

   @Test
   public void testRetrieveMetadata() throws Exception {
      RemoteCache<Integer, AccountHS> cache = clients.get(0).getCache();
      cache.put(1, newAccount(1), 1, TimeUnit.DAYS);
      cache.put(2, newAccount(2), 2, TimeUnit.MINUTES, 30, TimeUnit.SECONDS);
      cache.put(3, newAccount(3));

      try (CloseableIterator<Entry<Object, MetadataValue<Object>>> iterator = cache.retrieveEntriesWithMetadata(null, 10)) {
         Entry<Object, MetadataValue<Object>> entry = iterator.next();
         if ((int) entry.getKey() == 1) {
            assertEquals(24 * 3600, entry.getValue().getLifespan());
            assertEquals(-1, entry.getValue().getMaxIdle());
         }
         if ((int) entry.getKey() == 2) {
            assertEquals(2 * 60, entry.getValue().getLifespan());
            assertEquals(30, entry.getValue().getMaxIdle());
         }
         if ((int) entry.getKey() == 3) {
            assertEquals(-1, entry.getValue().getLifespan());
            assertEquals(-1, entry.getValue().getMaxIdle());
         }
      }
   }

   static final class ToHexConverterFactory implements KeyValueFilterConverterFactory<Integer, Integer, String> {
      @Override
      public KeyValueFilterConverter<Integer, Integer, String> getFilterConverter() {
         return new HexFilterConverter();
      }

      static class HexFilterConverter extends AbstractKeyValueFilterConverter<Integer, Integer, String> implements Serializable {
         @Override
         public String filterAndConvert(Integer key, Integer value, Metadata metadata) {
            return Integer.toHexString(value);
         }
      }

   }

   static final class SubstringFilterFactory implements ParamKeyValueFilterConverterFactory<String, String, String> {

      public static final int DEFAULT_LENGTH = 20;

      @Override
      public KeyValueFilterConverter<String, String, String> getFilterConverter(Object[] params) {
         return new SubstringFilterConverter(params);
      }


      static class SubstringFilterConverter extends AbstractKeyValueFilterConverter<String, String, String> implements Serializable {
         private final int length;

         public SubstringFilterConverter(Object[] params) {
            this.length = (int) (params == null || params.length == 0 ? DEFAULT_LENGTH : params[0]);
         }

         @Override
         public String filterAndConvert(String key, String value, Metadata metadata) {
            return value.substring(0, length);
         }
      }
   }

   private Set<Integer> getKeysFromSegments(Set<Integer> segments) {
      RemoteCacheManager remoteCacheManager = clients.get(0);
      RemoteCache<Integer, ?> cache = remoteCacheManager.getCache();
      Marshaller marshaller = clients.get(0).getMarshaller();
      KeyPartitioner keyPartitioner = TestingUtil.extractComponent(cache(0), KeyPartitioner.class);
      Set<Integer> keys = cache.keySet();
      return keys.stream()
                 .filter(b -> segments.contains(keyPartitioner.getSegment(toByteBuffer(b, marshaller))))
                 .collect(Collectors.toSet());
   }

}
