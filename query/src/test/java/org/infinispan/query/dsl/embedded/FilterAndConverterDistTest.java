package org.infinispan.query.dsl.embedded;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.query.dsl.embedded.impl.FilterAndConverter;
import org.infinispan.query.test.Person;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.dsl.embedded.FilterAndConverterDistTest")
public class FilterAndConverterDistTest extends MultipleCacheManagersTest {

   protected final int numNodes;

   protected FilterAndConverterDistTest(int numNodes) {
      this.numNodes = numNodes;
   }

   public FilterAndConverterDistTest() {
      this(3);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cfgBuilder.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      createClusteredCaches(numNodes, cfgBuilder);
   }

   @Test
   public void testFilter() {
      final boolean isClustered = cache(0).getCacheConfiguration().clustering().cacheMode().isClustered();
      for (int i = 0; i < 10; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 30);

         Cache<Object, Person> cache = cache(i % numNodes);
         Object key = isClustered ? new MagicKey(cache) : i;
         cache.put(key, value);
      }

      FilterAndConverter filterAndConverter = new FilterAndConverter<Object, Person>("from org.infinispan.query.test.Person where blurb is null and age <= 31", ReflectionMatcher.class);

      CloseableIterator<Map.Entry<Object, ObjectFilter.FilterResult>> iterator = cache(0).getAdvancedCache().filterEntries(filterAndConverter).converter(filterAndConverter).iterator();
      Map<Object, ObjectFilter.FilterResult> results = mapFromIterator(iterator);

      assertEquals(2, results.size());
      for (ObjectFilter.FilterResult p : results.values()) {
         assertNull(((Person) p.getInstance()).getBlurb());
         assertTrue(((Person) p.getInstance()).getAge() <= 31);
      }
   }

   /**
    * Iterates over all the entries provided by the iterator and puts them in a Map.
    */
   private Map<Object, ObjectFilter.FilterResult> mapFromIterator(CloseableIterator<Map.Entry<Object, ObjectFilter.FilterResult>> iterator) {
      try {
         Map<Object, ObjectFilter.FilterResult> result = new HashMap<Object, ObjectFilter.FilterResult>();
         while (iterator.hasNext()) {
            Map.Entry<Object, ObjectFilter.FilterResult> entry = iterator.next();
            result.put(entry.getKey(), entry.getValue());
         }
         return result;
      } finally {
         iterator.close();
      }
   }
}
