package org.infinispan.stream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.KeyFilterAsKeyValueFilter;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Base class to test if evicted entries are not returned via stream
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.BaseStreamIteratorEvictionTest")
public abstract class BaseStreamIteratorEvictionTest extends BaseSetupStreamIteratorTest {
   public BaseStreamIteratorEvictionTest(boolean tx, CacheMode mode) {
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
      // Now we insert a value that will expire in 2 seconds
      cache.put("expired", "this shouldn't be returned", expectedTime, TimeUnit.SECONDS);

      // We have to wait the time limit to make sure it is evicted before proceeding
      Thread.sleep(TimeUnit.SECONDS.toMillis(expectedTime) + 50);

      cache.getAdvancedCache().filterEntries(new KeyFilterAsKeyValueFilter<Object, String>(
            new CollectionKeyFilter<>(Collections.emptySet())));

      Map<Object, String> results;
      try (Stream<CacheEntry<Object, String>> stream = cache.getAdvancedCache().cacheEntrySet().stream()) {
         results = mapFromStream(stream);
      }

      assertEquals(valuesInserted, results);
   }
}
