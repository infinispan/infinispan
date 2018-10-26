package org.infinispan.query.partition;

import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESMETADATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_LOCKING_CACHENAME;

import java.util.List;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

/**
 * @since 9.3
 */
@Test(groups = "functional", testName = "query.partitionhandling.NonSharedIndexWithLocalCachesTest")
public class NonSharedIndexWithLocalCachesTest extends NonSharedIndexTest {

   @Override
   protected ConfigurationBuilder cacheConfiguration() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.indexing()
            .index(Index.PRIMARY_OWNER)
            .addProperty("default.indexmanager", "near-real-time")
            .addProperty("default.directory_provider", "infinispan");
      return configurationBuilder;
   }

   protected void postConfigure(List<EmbeddedCacheManager> cacheManagers) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().index(Index.NONE);
      Configuration configuration = builder.build();

      cacheManagers.forEach(cm -> {
         cm.defineConfiguration(DEFAULT_LOCKING_CACHENAME, configuration);
         cm.defineConfiguration(DEFAULT_INDEXESDATA_CACHENAME, configuration);
         cm.defineConfiguration(DEFAULT_INDEXESMETADATA_CACHENAME, configuration);
      });
   }

}
