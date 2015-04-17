package org.infinispan.query.blackbox;

import org.infinispan.hibernate.search.spi.InfinispanIntegration;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Tests verifying Querying on REPL cache configured with InfinispanIndexManager and infinispan directory provider.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheWithInfinispanDirectoryTest")
public class ClusteredCacheWithInfinispanDirectoryTest extends ClusteredCacheTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
      cacheCfg.indexing()
            .index(Index.LOCAL)
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler");
      cacheCfg.clustering().stateTransfer().fetchInMemoryState(true);
      enhanceConfig(cacheCfg);
      List<Cache<String, Person>> caches = createClusteredCaches(2, cacheCfg);
      cache1 = caches.get(0);
      cache2 = caches.get(1);

      ConfigurationBuilder cacheCfg1 = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cacheCfg1.clustering().stateTransfer().fetchInMemoryState(true);
      cacheManagers.get(0).defineConfiguration(InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME, cacheCfg1.build());
      cacheManagers.get(0).defineConfiguration( InfinispanIntegration.DEFAULT_LOCKING_CACHENAME, cacheCfg1.build());

      cacheManagers.get(1).defineConfiguration(InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME, cacheCfg1.build());
      cacheManagers.get(1).defineConfiguration( InfinispanIntegration.DEFAULT_LOCKING_CACHENAME, cacheCfg1.build());
   }

}
