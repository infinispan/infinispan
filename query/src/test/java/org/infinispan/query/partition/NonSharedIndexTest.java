package org.infinispan.query.partition;

import java.util.List;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.testng.annotations.Test;

/**
 * @since 9.3
 */
@Test(groups = "functional", testName = "query.partitionhandling.NonSharedIndexTest")
public class NonSharedIndexTest extends SharedIndexTest {

   @Override
   protected ConfigurationBuilder cacheConfiguration() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.indexing().index(Index.PRIMARY_OWNER)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return configurationBuilder;
   }

   @Override
   protected IndexedQueryMode getIndexedQueryMode() {
      return IndexedQueryMode.BROADCAST;
   }

   @Override
   protected void postConfigure(List<EmbeddedCacheManager> cacheManagers) {
   }
}
