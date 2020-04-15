package org.infinispan.query.blackbox;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.testng.annotations.Test;

/**
 * @since 9.1
 */
@Test(groups = {"functional"}, testName = "query.blackbox.ClusteredDistCacheTest")
public class ClusteredDistCacheTest extends ClusteredCacheTest {

   protected Cache<Object, Person> cache3;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheCfg.indexing()
            .enable()
            .addIndexedEntity(Person.class)
            .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP)
            .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName());

      cacheCfg.memory().storageType(storageType);
      enhanceConfig(cacheCfg);
      createClusteredCaches(3, QueryTestSCI.INSTANCE, cacheCfg);
      cache1 = cache(0);
      cache2 = cache(1);
      cache3 = cache(2);
   }

}
