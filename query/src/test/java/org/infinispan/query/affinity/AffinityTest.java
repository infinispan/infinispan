package org.infinispan.query.affinity;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.lucene.IndexScopedKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.test.Person;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESMETADATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_LOCKING_CACHENAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author gustavonalle
 * @since 8.1
 */
@Test(groups = "functional", testName = "query.AffinityTest")
public class AffinityTest extends MultipleCacheManagersTest {

   Cache<String, Person> cache1, cache2, cache3;
   private ConfigurationBuilder cacheCfg;

   @Override
   protected void createCacheManagers() throws Throwable {
      cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheCfg.clustering().hash().numSegments(10).numOwners(1);
      cacheCfg.indexing()
            .index(Index.ALL)
            .addProperty("hibernate.search.default.directory_provider", "infinispan")
            .addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT")
            .addProperty("infinispan.index.affinity", "enabled")
      ;
      List<Cache<String, Person>> caches = createClusteredCaches(3, cacheCfg);
      cache1 = caches.get(0);
      cache2 = caches.get(1);
      cache3 = caches.get(2);
   }

   public void shouldHaveIndexAffinity() throws Exception {
      IntStream.rangeClosed(0, 50).boxed().forEach(i -> cache1.put(i.toString(), new Person("person" + i, "", i)));

      checkAffinity();

      addClusterEnabledCacheManager(cacheCfg);
      waitForClusterToForm();

      IntStream.rangeClosed(51, 100).boxed().forEach(i -> cache1.put(i.toString(), new Person("person" + i, "", i)));

      checkAffinity();

      assertEquals(101, cache1.size());

      CacheQuery q = Search.getSearchManager(cache2).getQuery(new MatchAllDocsQuery(), Person.class);

      assertEquals(101, q.list().size());

   }

   private void checkAffinity() {
      for (EmbeddedCacheManager clusterMember : cacheManagers) {
         checkAffinity(clusterMember.getCache(DEFAULT_INDEXESDATA_CACHENAME));
         checkAffinity(clusterMember.getCache(DEFAULT_INDEXESMETADATA_CACHENAME));
         checkAffinity(clusterMember.getCache(DEFAULT_LOCKING_CACHENAME));
      }
   }

   private void checkAffinity(Cache<IndexScopedKey, ?> indexCache) {
      DataContainer<IndexScopedKey, ?> dataContainer = indexCache.getAdvancedCache().getDataContainer();
      ConsistentHash consistentHash = indexCache.getAdvancedCache().getDistributionManager().getConsistentHash();
      Address address = indexCache.getAdvancedCache().getRpcManager().getAddress();
      Set<Integer> ownedSegments = consistentHash.getPrimarySegmentsForOwner(address);
      dataContainer.forEach(entry -> {
         int segmentAffinity = entry.getKey().getAffinitySegmentId();
         assertTrue(ownedSegments.contains(segmentAffinity));
      });
   }

}