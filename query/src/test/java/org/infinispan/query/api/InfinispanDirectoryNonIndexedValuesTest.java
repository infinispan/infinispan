package org.infinispan.query.api;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Testing Non-indexed values on InfinispanDirectory.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.api.InfinispanDirectoryNonIndexedValuesTest")
public class InfinispanDirectoryNonIndexedValuesTest extends NonIndexedValuesTest {

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(isTransactional());
      c.indexing()
            .enable()
            .addIndexedEntity(TestEntity.class)
            .addIndexedEntity(AnotherTestEntity.class)
            .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP)
            .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName());
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, c);
   }

   protected boolean isTransactional() {
      return false;
   }
}
