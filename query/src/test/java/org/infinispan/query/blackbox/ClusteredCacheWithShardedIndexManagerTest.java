package org.infinispan.query.blackbox;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.distribution.ch.impl.AffinityPartitioner;
import org.infinispan.query.affinity.ShardIndexManager;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @author gustavonalle
 * @since 8.2
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheWithShardedIndexManagerTest")
public class ClusteredCacheWithShardedIndexManagerTest extends ClusteredCacheTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
      cacheCfg.clustering().hash().keyPartitioner(new AffinityPartitioner());
      cacheCfg.indexing()
              .index(Index.ALL)
              .addIndexedEntity(Person.class)
              .addProperty("default.indexmanager", ShardIndexManager.class.getName())
              .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
              .addProperty("lucene_version", "LUCENE_CURRENT");
      enhanceConfig(cacheCfg);
      List<Cache<String, Person>> caches = createClusteredCaches(2, cacheCfg);
      cache1 = caches.get(0);
      cache2 = caches.get(1);
   }

}
