package org.infinispan.query.distributed;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.query.test.elasticsearch.ElasticSearchCluster;
import org.infinispan.query.test.elasticsearch.ElasticSearchCluster.ElasticSearchClusterBuilder;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

/**
 * @since 9.0
 */
public class ElasticSearchMassIndexingTest extends DistributedMassIndexingTest {


   private ElasticSearchCluster elasticSearchCluster;

   @BeforeTest
   protected void prepareElasticSearch() throws IOException {
      elasticSearchCluster = new ElasticSearchClusterBuilder()
            .withNumberNodes(2)
            .addPlugin(DeleteByQueryPlugin.class)
            .build();
      elasticSearchCluster.start();
   }

   @AfterTest
   public void tearDownElasticSearch() throws IOException {
      elasticSearchCluster.stop();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cacheCfg.indexing().index(Index.LOCAL)
            .addIndexedEntity(Car.class)
            .addProperty("default.indexmanager", "elasticsearch")
            .addProperty("default.elasticsearch.refresh_after_write", "true")
            .addProperty("default.elasticsearch.host", elasticSearchCluster.getConnectionString())
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      List<Cache<Object, Object>> cacheList = createClusteredCaches(NUM_NODES, cacheCfg);
      waitForClusterToForm(neededCacheNames);

      caches.addAll(cacheList.stream().collect(Collectors.toList()));
   }

}
