package org.infinispan.query.dsl.embedded;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.iteration.impl.EntryRetriever;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.query.dsl.embedded.impl.FilterAndConverter;
import org.infinispan.query.test.Person;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.dsl.embedded.FilterAndConverterDistTest")
public class FilterAndConverterDistTest extends MultipleCacheManagersTest {

   private final String CACHE_NAME = getClass().getName();

   private final int NUM_NODES = 3;

   public FilterAndConverterDistTest() {
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = new ConfigurationBuilder();
      cfgBuilder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL)
            .clustering().hash().numOwners(2)
            .stateTransfer().chunkSize(50);
      createClusteredCaches(NUM_NODES, CACHE_NAME, cfgBuilder);
   }

   @Test
   public void testFilter() {
      for (int i = 0; i < 10; ++i) {
         Cache<Object, Person> cache = cache(i % NUM_NODES, CACHE_NAME);
         Object key = new MagicKey(cache);
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 30);
         cache.put(key, value);
      }

      EntryRetriever<MagicKey, Person> retriever = cache(0, CACHE_NAME).getAdvancedCache().getComponentRegistry().getComponent(EntryRetriever.class);

      FilterAndConverter filterAndConverter = new FilterAndConverter<MagicKey, Person, Person>("from org.infinispan.query.test.Person where blurb is null and age <= 31", ReflectionMatcher.class);

      CloseableIterator<Map.Entry<MagicKey, Person>> iterator = retriever.retrieveEntries(filterAndConverter, filterAndConverter, null);
      Map<MagicKey, Person> results = mapFromIterator(iterator);

      assertEquals(2, results.size());
      for (Person p : results.values()) {
         assertNull(p.getBlurb());
         assertTrue(p.getAge() <= 31);
      }
   }

   /**
    * Iterates over all the entries provided by the iterator and puts them in a Map. If the iterator implements
    * Closeable it will close it before returning.
    */
   private <K, V> Map<K, V> mapFromIterator(Iterator<Map.Entry<K, V>> iterator) {
      try {
         Map<K, V> result = new HashMap<K, V>();
         while (iterator.hasNext()) {
            Map.Entry<K, V> entry = iterator.next();
            result.put(entry.getKey(), entry.getValue());
         }
         return result;
      } finally {
         if (iterator instanceof Closeable) {
            try {
               ((Closeable) iterator).close();
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
         }
      }
   }
}