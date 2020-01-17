package org.infinispan.query.it;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.configuration.cache.IndexingConfigurationBuilder;

/**
 * Recommended configuration properties to run integration tests
 * with the Hibernate Search / Elasticsearch indexing option.
 */
public class ElasticsearchTesting {

   public static Map<String, String> getConfigProperties() {
      Map<String, String> indexConfig = new HashMap<>();
      indexConfig.put("default.indexmanager", "elasticsearch");
      indexConfig.put("default.elasticsearch.required_index_status", "yellow");
      indexConfig.put("default.elasticsearch.index_schema_management_strategy", "drop-and-create-and-drop");
      indexConfig.put("default.elasticsearch.refresh_after_write", "true");
      indexConfig.put("default.elasticsearch.dynamic_mapping", "true");
      indexConfig.put("lucene_version", "LUCENE_CURRENT");
      indexConfig.put("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler");
      return indexConfig;
   }

   public static void applyTestProperties(IndexingConfigurationBuilder indexing) {
      getConfigProperties().forEach( (k,v) -> indexing.addProperty(k, v));
   }

}
