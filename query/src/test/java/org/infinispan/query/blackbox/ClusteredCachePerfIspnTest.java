package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.CustomKey3;
import org.infinispan.query.test.CustomKey3Transformer;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Testing the functionality of NRT index manager for clustered caches.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCachePerfIspnTest")
public class ClusteredCachePerfIspnTest extends ClusteredCacheTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
      cacheCfg.indexing()
            .enable()
            .addIndexedEntity(Person.class)
            .addKeyTransformer(CustomKey3.class, CustomKey3Transformer.class)
              .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP)
            .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName())
            .addProperty(SearchConfig.THREAD_POOL_SIZE, "6")
            .addProperty(SearchConfig.QUEUE_COUNT, "6")
            .addProperty(SearchConfig.QUEUE_SIZE, "4096")
            .addProperty(SearchConfig.COMMIT_INTERVAL, "10000")
            // changing the refresh interval would make the the test not working
            //.addProperty(SearchConfig.REFRESH_INTERVAL, "10000")
            .addProperty(SearchConfig.SHARDING_STRATEGY, SearchConfig.HASH)
            .addProperty(SearchConfig.NUMBER_OF_SHARDS, "6");

      enhanceConfig(cacheCfg);

      for (int i = 0; i < 2; i++) {
         ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
         holder.getGlobalConfigurationBuilder()
               .clusteredDefault()
               .defaultCacheName(TestCacheManagerFactory.DEFAULT_CACHE_NAME)
               .serialization().addContextInitializer(QueryTestSCI.INSTANCE);

         holder.getNamedConfigurationBuilders().put(TestCacheManagerFactory.DEFAULT_CACHE_NAME, cacheCfg);

         addClusterEnabledCacheManager(holder);
      }

      cache1 = manager(0).getCache();
      cache2 = manager(1).getCache();
   }
}
