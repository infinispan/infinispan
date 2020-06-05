package org.infinispan.query.timeout;

import java.util.concurrent.TimeUnit;

import org.hibernate.search.util.common.SearchTimeoutException;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.timeout.DistributedIndexedTimeoutTest")
public class DistributedIndexedTimeoutTest extends MultipleCacheManagersTest {
   protected Cache<Integer, Person> cache1;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheCfg.indexing().enable()
            .addIndexedEntity(Person.class)
            .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP);
      createClusteredCaches(2, QueryTestSCI.INSTANCE, cacheCfg);
      cache1 = cache(0);
   }

   @BeforeMethod
   public void populate() {
      TestHelper.populate(cache1, 20);
   }

   @Test(expectedExceptions = SearchTimeoutException.class)
   public void testTimeout() {
      TestHelper.runFullTextQueryWithTimeout(cache1, 1, TimeUnit.NANOSECONDS);
   }
}
