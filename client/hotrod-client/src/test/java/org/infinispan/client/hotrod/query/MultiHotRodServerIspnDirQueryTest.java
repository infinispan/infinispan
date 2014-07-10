package org.infinispan.client.hotrod.query;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * Verifying the functionality of queries using a local infinispan directory provider.
 *
 * @author Anna Manukyan
 */
@Test(testName = "client.hotrod.query.MultiHotRodServerIspnDirQueryTest", groups = "functional")
public class MultiHotRodServerIspnDirQueryTest extends MultiHotRodServerQueryTest {

   protected static final String TEST_CACHE = "queryableCache";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      createHotRodServers(2, defaultConfiguration);

      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
      builder.indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "infinispan")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      for (EmbeddedCacheManager cm : cacheManagers) {
         cm.defineConfiguration(TEST_CACHE, builder.build());
      }

      waitForClusterToForm();

      remoteCache0 = client(0).getCache(TEST_CACHE);
      remoteCache1 = client(1).getCache(TEST_CACHE);
   }
}
