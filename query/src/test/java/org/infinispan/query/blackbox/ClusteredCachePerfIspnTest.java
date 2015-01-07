package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
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
      EmbeddedCacheManager cm1 = addClusterEnabledCacheManager(new ConfigurationBuilder());
      EmbeddedCacheManager cm2 = addClusterEnabledCacheManager(new ConfigurationBuilder());

      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
      cacheCfg.indexing()
            .index(Index.ALL)
            .addProperty("default.indexmanager", "near-real-time")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("default.directory_provider", "infinispan")
            .addProperty("default.chunk_size", "128000")
            .addProperty("default.indexwriter.merge_factor", "30")
            .addProperty("default.indexwriter.merge_max_size", "1024")
            .addProperty("default.indexwriter.ram_buffer_size", "64")
            .addProperty("default.sharding_strategy.nbr_of_shards", "6");

      enhanceConfig(cacheCfg);

      cm1.defineConfiguration("perfConf", cacheCfg.build());
      cm2.defineConfiguration("perfConf", cacheCfg.build());

      cache1 = cm1.getCache("perfConf");
      cache2 = cm2.getCache("perfConf");
   }
}
