package org.infinispan.query.api;

import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.hibernate.search.elasticsearch.impl.ElasticsearchIndexManager;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.elasticsearch.ElasticSearchCluster;
import org.infinispan.query.test.elasticsearch.ElasticSearchCluster.ElasticSearchClusterBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @since 9.0
 */
@Test(groups = "functional", testName = "query.api.ElasticSearchNonIndexedValuesTest")
public class ElasticSearchNonIndexedValuesTest extends NonIndexedValuesTest {

   private ElasticSearchCluster elasticSearchCluster;

   @Override
   protected void setup() throws Exception {
      elasticSearchCluster = new ElasticSearchClusterBuilder()
            .withNumberNodes(2)
            .waitingForGreen(2000L)
            .addPlugin(DeleteByQueryPlugin.class)
            .build();
      elasticSearchCluster.start();
      super.setup();
   }

   @Override
   protected void teardown() {
      elasticSearchCluster.stop();
      super.teardown();
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(isTransactional());
      c.indexing()
            .index(Index.LOCAL)
            .addIndexedEntity(TestEntity.class)
            .addIndexedEntity(AnotherTestEntity.class)
            .addProperty("default.indexmanager", ElasticsearchIndexManager.class.getName())
            .addProperty("default.elasticsearch.host", elasticSearchCluster.getConnectionString())
            .addProperty("default.elasticsearch.refresh_after_write", "true")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(c);
   }

   protected boolean isTransactional() {
      return false;
   }
}
