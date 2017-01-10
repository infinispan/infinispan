package org.infinispan.query.blackbox;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.hibernate.search.spi.InfinispanIntegration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

/**
 * Testing the functionality of NRT index manager for clustered caches.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCachePerfIspnTest")
public class ClusteredCachePerfIspnTest extends ClusteredCacheTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
      cacheCfg.indexing()
            .index(Index.ALL)
            .addIndexedEntity(Person.class)
            .addProperty("default.indexmanager", "near-real-time")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("default.directory_provider", "infinispan")
            .addProperty("default.chunk_size", "128000")
            .addProperty("default.indexwriter.merge_factor", "30")
            .addProperty("default.indexwriter.merge_max_size", "1024")
            .addProperty("default.indexwriter.ram_buffer_size", "64")
            .addProperty("default.sharding_strategy.nbr_of_shards", "6")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      enhanceConfig(cacheCfg);
      ConfigurationBuilder indexCfg = new ConfigurationBuilder();
      for(int i = 0; i < 2; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(cacheCfg);
         cm.defineConfiguration(InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME, indexCfg.build());
         cm.defineConfiguration(InfinispanIntegration.DEFAULT_INDEXESMETADATA_CACHENAME, indexCfg.build());
         cm.defineConfiguration(InfinispanIntegration.DEFAULT_LOCKING_CACHENAME, indexCfg.build());
      }

      cache1 = manager(0).getCache();
      cache2 = manager(1).getCache();
   }
}
