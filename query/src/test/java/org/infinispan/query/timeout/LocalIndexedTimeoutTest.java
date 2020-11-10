package org.infinispan.query.timeout;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.SearchTimeoutException;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.timeout.LocalIndexedTimeoutTest")
public class LocalIndexedTimeoutTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cacheCfg = getDefaultStandaloneCacheConfig(false);
      cacheCfg.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class);
      return TestCacheManagerFactory.createCacheManager(cacheCfg);
   }

   @BeforeMethod
   public void populate() {
      TestHelper.populate(cache, 10000);
   }

   @Test(expectedExceptions = SearchTimeoutException.class)
   public void testTimeout() {
      TestHelper.runFullTextQueryWithTimeout(cache, 1, TimeUnit.NANOSECONDS);
   }
}
