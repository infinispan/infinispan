package org.infinispan.query.blackbox;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.CustomKey3;
import org.infinispan.query.test.CustomKey3Transformer;
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
            .storage(LOCAL_HEAP)
            .addKeyTransformer(CustomKey3.class, CustomKey3Transformer.class)
            .addIndexedEntity(Person.class)
            .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName());

      enhanceConfig(cacheCfg);
      createClusteredCaches(3, QueryTestSCI.INSTANCE, cacheCfg);
      cache1 = cache(0);
      cache2 = cache(1);
      cache3 = cache(2);
   }

}
