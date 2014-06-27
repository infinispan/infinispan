package org.infinispan.query.dsl.embedded;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.iteration.impl.EntryRetriever;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.query.dsl.embedded.impl.FilterAndConverter;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
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
@Test(groups = "functional", testName = "query.dsl.embedded.FilterAndConverterLocalTest")
public class FilterAndConverterLocalTest extends SingleCacheManagerTest {

   public FilterAndConverterLocalTest() {
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Test
   public void testFilter() {
      for (int i = 0; i < 10; ++i) {
         Cache<Object, Person> cache = cache();
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 30);
         cache.put(i, value);
      }

      EntryRetriever<MagicKey, Person> retriever = cache().getAdvancedCache().getComponentRegistry().getComponent(EntryRetriever.class);

      FilterAndConverter filterAndConverter = new FilterAndConverter<MagicKey, Person>("from org.infinispan.query.test.Person where blurb is null and age <= 31", ReflectionMatcher.class);

      CloseableIterator<Map.Entry<MagicKey, ObjectFilter.FilterResult>> iterator = retriever.retrieveEntries(filterAndConverter, filterAndConverter, null, null);
      Map<MagicKey, ObjectFilter.FilterResult> results = mapFromIterator(iterator);

      assertEquals(2, results.size());
      for (ObjectFilter.FilterResult p : results.values()) {
         assertNull(((Person) p.getInstance()).getBlurb());
         assertTrue(((Person) p.getInstance()).getAge() <= 31);
      }
   }

   /**
    * Iterates over all the entries provided by the iterator and puts them in a Map. If the iterator implements
    * Closeable it will close it before returning.
    */
   private <K, V> Map<K, ObjectFilter.FilterResult> mapFromIterator(Iterator<Map.Entry<K, ObjectFilter.FilterResult>> iterator) {
      try {
         Map<K, ObjectFilter.FilterResult> result = new HashMap<K, ObjectFilter.FilterResult>();
         while (iterator.hasNext()) {
            Map.Entry<K, ObjectFilter.FilterResult> entry = iterator.next();
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