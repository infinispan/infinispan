package org.infinispan.iteration;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.KeyFilterAsKeyValueFilter;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Base class to test if evicted entries are not returned via entry retrieval
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "iteration.BaseEntryRetrieverEvictionTest")
public abstract class BaseEntryRetrieverEvictionTest extends BaseSetupEntryRetrieverTest {
   public BaseEntryRetrieverEvictionTest(boolean tx, CacheMode mode) {
      super(tx, mode);
   }

   public void testExpiredEntryNotReturned() throws InterruptedException {
      Cache<Object, String> cache = cache(0, CACHE_NAME);
      // First put some values in there
      Map<Object, String> valuesInserted = new LinkedHashMap<Object, String>();
      for (int i = 0; i < 5; ++i) {
         Object key = i;
         String value = key + " stay in cache";
         cache.put(key, value);
         valuesInserted.put(key, value);
      }

      int expectedTime = 2;
      long beforeInsert = System.nanoTime();
      // Now we insert a value that will expire in 2 seconds
      cache.put("expired", "this shouldn't be returned", expectedTime, TimeUnit.SECONDS);

      // We have to wait the time limit to make sure it is evicted before proceeding
      waitUntil(beforeInsert + TimeUnit.SECONDS.toNanos(2) + 50);

      cache.getAdvancedCache().filterEntries(new KeyFilterAsKeyValueFilter<Object, String>(
            new CollectionKeyFilter<>(Collections.emptySet())));

      Map<Object, String> results;
      try (EntryIterable<Object, String> iterable = cache.getAdvancedCache().filterEntries(new KeyFilterAsKeyValueFilter<Object, String>(
            new CollectionKeyFilter<>(Collections.emptySet())))) {
         results = mapFromIterable(iterable);
      }

      assertEquals(valuesInserted, results);
   }

   private void waitUntil(long expectedTime) throws InterruptedException {
      while (expectedTime - System.nanoTime() > 0) {
          Thread.sleep(100);
      }
   }
}
