package org.infinispan.query.distributed;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.distribution.ch.impl.AffinityPartitioner;
import org.infinispan.query.affinity.ShardIndexManager;
import org.infinispan.query.queries.faceting.Car;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author gustavonalle
 * @since 8.2
 */
@Test(groups = "functional", testName = "query.distributed.ShardedIndexManagerMassIndexTest")
public class ShardedIndexManagerMassIndexTest extends DistributedMassIndexingTest {

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cacheCfg.clustering().hash().keyPartitioner(new AffinityPartitioner());
      cacheCfg.indexing()
              .index(Index.ALL)
              .addIndexedEntity(Car.class)
              .addProperty("default.indexmanager", ShardIndexManager.class.getName())
              .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
              .addProperty("lucene_version", "LUCENE_CURRENT");
      List<Cache<Object, Object>> cacheList = createClusteredCaches(NUM_NODES, cacheCfg);
      waitForClusterToForm(neededCacheNames);

      caches.addAll(cacheList.stream().collect(Collectors.toList()));
   }

}
