package org.infinispan.query.blackbox;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.hibernate.search.elasticsearch.impl.ElasticsearchIndexManager;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.distribution.ch.impl.AffinityPartitioner;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.elasticsearch.ElasticSearchCluster;
import org.infinispan.query.test.elasticsearch.ElasticSearchCluster.ElasticSearchClusterBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @since 9.0
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheWithElasticsearchIndexManagerTest")
public class ClusteredCacheWithElasticsearchIndexManagerTest extends ClusteredCacheTest {

   private ElasticSearchCluster elasticSearchCluster;

   @BeforeClass
   public void prepareElasticSearch() throws IOException {
      elasticSearchCluster = new ElasticSearchClusterBuilder()
            .withNumberNodes(2)
            .refreshInterval(100L)
            .addPlugin(DeleteByQueryPlugin.class)
            .build();
      elasticSearchCluster.start();
   }

   @AfterClass
   public void tearDownElasticSearch() throws IOException {
      elasticSearchCluster.stop();
   }

   @Override
   public void testCombinationOfFilters() throws Exception {
      // Not supported by hibernate search
   }

   @Override
   public void testFullTextFilterOnOff() throws Exception {
      // Not supported by hibernate search
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
      cacheCfg.clustering().hash().keyPartitioner(new AffinityPartitioner());
      cacheCfg.indexing()
            .index(Index.LOCAL)
            .addIndexedEntity(Person.class)
            .addProperty("default.indexmanager", ElasticsearchIndexManager.class.getName())
            .addProperty("default.elasticsearch.refresh_after_write", "true")
            .addProperty("default.elasticsearch.host", elasticSearchCluster.getConnectionString())
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler");
      List<Cache<String, Person>> caches = createClusteredCaches(2, cacheCfg);
      cache1 = caches.get(0);
      cache2 = caches.get(1);
   }

}
