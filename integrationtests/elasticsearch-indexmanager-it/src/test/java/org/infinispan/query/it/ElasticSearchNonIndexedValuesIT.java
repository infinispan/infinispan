package org.infinispan.query.it;


import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.api.AnotherTestEntity;
import org.infinispan.query.api.NonIndexedValuesTest;
import org.infinispan.query.api.TestEntity;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/*
 * @since 9.0
 */
@Test(groups = "functional", testName = "query.api.ElasticSearchNonIndexedValuesIT")
public class ElasticSearchNonIndexedValuesIT extends NonIndexedValuesTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder cacheCfg = getDefaultStandaloneCacheConfig(isTransactional());
      cacheCfg.indexing()
            .enable()
            .addIndexedEntity(TestEntity.class)
            .addIndexedEntity(AnotherTestEntity.class);
      ElasticsearchTesting.applyTestProperties(cacheCfg.indexing());
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cacheCfg);
   }

   protected boolean isTransactional() {
      return false;
   }
}
