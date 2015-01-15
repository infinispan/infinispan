package org.infinispan.query.distributed;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.queries.faceting.Car;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Test the mass indexer on a REPL cache and shared index
 * @author gustavonalle
 * @since 7.1
 */
@Test(groups = "functional", testName = "query.distributed.SharedReplMassIndexTest")
public class SharedReplMassIndexTest extends DistributedMassIndexingTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cacheCfg.indexing()
            .index(Index.ALL)
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler");
      List<Cache<String, Car>> cacheList = createClusteredCaches(NUM_NODES, cacheCfg);

      waitForClusterToForm(neededCacheNames);

      for (Cache cache : cacheList) {
         caches.add(cache);
      }
   }
}
