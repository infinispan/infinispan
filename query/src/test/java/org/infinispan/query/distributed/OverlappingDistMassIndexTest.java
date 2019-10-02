package org.infinispan.query.distributed;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.test.Block;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.query.test.Transaction;
import org.testng.annotations.Test;

/**
 * Tests for entities sharing the same index in DIST caches.
 *
 * @author gustavonalle
 * @since 7.1
 */
@Test(groups = "functional", testName = "query.distributed.OverlappingDistMassIndexTest")
public class OverlappingDistMassIndexTest extends OverlappingIndexMassIndexTest {

   @Override
   @SuppressWarnings("unchecked")
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheCfg
            .indexing()
            .index(Index.PRIMARY_OWNER)
            .addIndexedEntity(Transaction.class)
            .addIndexedEntity(Block.class)
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      List<Cache<String, Object>> cacheList = createClusteredCaches(NUM_NODES, QueryTestSCI.INSTANCE, cacheCfg);

      waitForClusterToForm(getDefaultCacheName());

      for (Cache cache : cacheList) {
         caches.add(cache);
      }
   }
}
