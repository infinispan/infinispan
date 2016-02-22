package org.infinispan.query.api;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.affinity.ShardIndexManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 8.2
 */
@Test(groups = "functional", testName = "query.api.ShardedIndexManagerNonIndexedValuesTest")
public class ShardedIndexManagerNonIndexedValuesTest extends NonIndexedValuesTest {

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(isTransactional());
      c.indexing()
              .index(Index.LOCAL)
              .addIndexedEntity(TestEntity.class)
              .addIndexedEntity(AnotherTestEntity.class)
              .addProperty("default.indexmanager", ShardIndexManager.class.getName())
              .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
              .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(c);
   }

   protected boolean isTransactional() {
      return false;
   }
}
