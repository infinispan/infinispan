package org.infinispan.client.hotrod.query;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * Verifies the functionality of the Queries in case of REPL infinispan directory_provider for clustered Hotrod servers.
 *
 * @author Anna Manukyan
 */
@Test(testName = "client.hotrod.query.MultiHotRodServerIspnDirReplQueryTest", groups = "functional")
public class MultiHotRodServerIspnDirReplQueryTest extends MultiHotRodServerIspnDirQueryTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfiguration = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      createHotRodServers(2, defaultConfiguration);

      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
      builder.indexing().index(Index.LOCAL)
            .addProperty("default.directory_provider", "infinispan")
            .addProperty("default.exclusive_index_use", "false")
            //.addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      for (EmbeddedCacheManager cm : cacheManagers) {
         cm.defineConfiguration(TEST_CACHE, builder.build());
      }

      waitForClusterToForm();

      remoteCache0 = client(0).getCache(TEST_CACHE);
      remoteCache1 = client(1).getCache(TEST_CACHE);
   }
}
