package org.infinispan.query.api;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
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
            .index(Index.LOCAL)
            .addIndexedEntity(TestEntity.class)
            .addIndexedEntity(AnotherTestEntity.class)
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(c);
   }

   protected boolean isTransactional() {
      return false;
   }
}
