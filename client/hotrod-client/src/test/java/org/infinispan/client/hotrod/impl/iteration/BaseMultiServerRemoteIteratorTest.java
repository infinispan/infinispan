package org.infinispan.client.hotrod.impl.iteration;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
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

      Marshaller marshaller = clients.iterator()
                                     .next()
                                     .getMarshaller();
      ConsistentHash consistentHash = advancedCache(0).getDistributionManager()
                                                      .getConsistentHash();

      assertKeysInSegment(entries, filterBySegments, marshaller, consistentHash::getSegment);
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

   private Set<Integer> getKeysFromSegments(Set<Integer> segments) {
      RemoteCacheManager remoteCacheManager = clients.get(0);
      RemoteCache<Integer, ?> cache = remoteCacheManager.getCache();
      Marshaller marshaller = clients.get(0)
                                     .getMarshaller();
      ConsistentHash hash = advancedCache(0).getDistributionManager()
                                            .getConsistentHash();
      Set<Integer> keys = cache.keySet();
      return keys.stream()
                 .filter(b -> segments.contains(hash.getSegment(toByteBuffer(b, marshaller))))
                 .collect(Collectors.toSet());
   }

}
