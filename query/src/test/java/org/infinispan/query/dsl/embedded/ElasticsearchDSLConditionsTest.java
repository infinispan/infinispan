package org.infinispan.query.dsl.embedded;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.infinispan.query.test.elasticsearch.ElasticSearchCluster;
import org.infinispan.query.test.elasticsearch.ElasticSearchCluster.ElasticSearchClusterBuilder;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * @since 9.0
 */
@Test(groups = "functional", testName = "query.dsl.embedded.ElasticsearchDSLConditionsTest")
public class ElasticsearchDSLConditionsTest extends ClusteredQueryDslConditionsTest {

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
   // HSEARCH-2389
   protected boolean testNullCollections() {
      return false;
   }

   @Override
   protected Map<String, String> getIndexConfig() {
      Map<String, String> indexConfig = new HashMap<>();
      indexConfig.put("default.indexmanager", "elasticsearch");
      indexConfig.put("default.elasticsearch.refresh_after_write", "true");
      indexConfig.put("default.elasticsearch.host", elasticSearchCluster.getConnectionString());
      indexConfig.put("lucene_version", "LUCENE_CURRENT");
      return indexConfig;
   }
}
