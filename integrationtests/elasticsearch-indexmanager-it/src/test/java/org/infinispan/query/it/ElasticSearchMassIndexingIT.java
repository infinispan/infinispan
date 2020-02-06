package org.infinispan.query.it;

import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.hibernate.search.spi.InfinispanIntegration;
import org.infinispan.query.distributed.DistributedMassIndexingTest;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.query.test.QueryTestSCI;
import org.testng.annotations.Test;

/**
 * @since 9.0
 */
@Test(groups = "functional", testName = "query.it.ElasticSearchMassIndexingIT")
public class ElasticSearchMassIndexingIT extends DistributedMassIndexingTest {

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cacheCfg.indexing()
            .enable()
            .addIndexedEntity(Car.class);
      ElasticsearchTesting.applyTestProperties(cacheCfg.indexing());
      List<Cache<Object, Object>> cacheList = createClusteredCaches(NUM_NODES, QueryTestSCI.INSTANCE, cacheCfg);
      ConfigurationBuilder indexCache = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      indexCache.clustering().stateTransfer().fetchInMemoryState(true);
      defineConfigurationOnAllManagers(InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME, indexCache);
      defineConfigurationOnAllManagers(InfinispanIntegration.DEFAULT_LOCKING_CACHENAME, indexCache);
      defineConfigurationOnAllManagers(InfinispanIntegration.DEFAULT_INDEXESMETADATA_CACHENAME, indexCache);

      waitForClusterToForm();

      caches.addAll(cacheList.stream().collect(Collectors.toList()));
   }
}
