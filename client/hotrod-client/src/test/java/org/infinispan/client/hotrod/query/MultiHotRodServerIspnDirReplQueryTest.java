package org.infinispan.client.hotrod.query;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

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
      createHotRodServers(3, defaultConfiguration);

      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.indexing().enable()
             .addIndexedEntity("sample_bank_account.User")
             .addProperty("default.directory_provider", "local-heap");

      for (EmbeddedCacheManager cm : cacheManagers) {
         cm.defineConfiguration(TEST_CACHE, builder.build());
         cm.getCache(TEST_CACHE);
      }

      waitForClusterToForm();

      remoteCache0 = client(0).getCache(TEST_CACHE);
      remoteCache1 = client(1).getCache(TEST_CACHE);
   }
}
