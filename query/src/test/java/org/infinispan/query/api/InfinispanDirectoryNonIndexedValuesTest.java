package org.infinispan.query.api;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
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
            .storage(LOCAL_HEAP)
            .addIndexedEntity(TestEntity.class)
            .addIndexedEntity(AnotherTestEntity.class);
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, c);
   }

   protected boolean isTransactional() {
      return false;
   }
}
