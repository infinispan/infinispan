package org.infinispan.query.blackbox;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

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
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class)
            .addKeyTransformer(CustomKey3.class, CustomKey3Transformer.class)
            .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName())
            .writer().ramBufferSize(54).commitInterval(10000).queueCount(6).queueSize(4096).threadPoolSize(6)
            .merge().factor(30).maxSize(1024);

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
