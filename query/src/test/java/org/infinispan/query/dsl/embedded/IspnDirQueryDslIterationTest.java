package org.infinispan.query.dsl.embedded;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verifies the functionality of DSL API iterations using ISPN directory provider.
 *
 * @author Anna Manukyan
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.dsl.embedded.IspnDirQueryDslIterationTest")
public class IspnDirQueryDslIterationTest extends QueryDslIterationTest {

   protected static final String TEST_CACHE_NAME = "custom";

   protected Cache<Object, Object> cache;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfiguration = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      createClusteredCaches(1, defaultConfiguration);

      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "infinispan");

      manager(0).defineConfiguration(TEST_CACHE_NAME, cfg.build());
      cache = manager(0).getCache(TEST_CACHE_NAME);
   }

   @Override
   protected Cache<Object, Object> getCacheForQuery() {
      return cache;
   }
}
