package org.infinispan.query.distributed;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.query.queries.faceting.Car;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Test for MassIndexer with a store
 *
 * @author gustavonalle
 * @since 7.1
 */
public class MassIndexingWithStoreTest extends DistributedMassIndexingTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cacheCfg.eviction().size(1L).strategy(EvictionStrategy.LRU);
      cacheCfg.persistence().passivation(true).addStore(DummyInMemoryStoreConfigurationBuilder.class).storeName(getClass().getSimpleName()).purgeOnStartup(true);
      cacheCfg.storeAsBinary().enable();
      cacheCfg.indexing()
            .index(Index.ALL)
            .addIndexedEntity(Car.class)
            .addProperty("default.directory_provider", "ram")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      List<Cache<String, Car>> cacheList = createClusteredCaches(2, cacheCfg);

      waitForClusterToForm(neededCacheNames);

      caches.addAll(cacheList.stream().collect(Collectors.toList()));

   }

   @Override
   public void testReindexing() throws Exception {
      Cache<String, Car> cache0 = caches.get(0);
      for (int i = 0; i < 10; i++) {
         cache0.put("CAR#" + i, new Car("Volkswagen", "white", 200));
      }
      rebuildIndexes();
      verifyFindsCar(10, "Volkswagen");
   }

}
