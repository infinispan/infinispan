package org.infinispan.query.blackbox;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
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
      cacheCfg.indexing().autoConfig(true)
            .index(Index.PRIMARY_OWNER)
            .addIndexedEntity(Person.class)
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler");

      cacheCfg.memory().storageType(storageType);
      enhanceConfig(cacheCfg);
      createClusteredCaches(3, QueryTestSCI.INSTANCE, cacheCfg);
      cache1 = cache(0);
      cache2 = cache(1);
      cache3 = cache(2);
   }

}
