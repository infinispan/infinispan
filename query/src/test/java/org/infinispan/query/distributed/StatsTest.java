package org.infinispan.query.distributed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.search.backend.lucene.index.LuceneIndexManager;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.query.core.stats.QueryStatistics;
import org.infinispan.query.core.stats.SearchStatistics;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.mapper.mapping.SearchIndexedEntity;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.query.test.Transaction;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @since 12.0
 */
@Test(groups = "functional", testName = "query.distributed.StatsTest")
public class StatsTest extends MultipleCacheManagersTest {

   // Wild guess: an empty index shouldn't be more than this many bytes
   private static final long MAX_EMPTY_INDEX_SIZE = 300L;

   private Cache<String, Object> cache0;
   private Cache<String, Object> cache1;
   private Cache<String, Object> cache2;
   private QueryStatistics queryStatistics0;
   private QueryStatistics queryStatistics1;
   private QueryStatistics queryStatistics2;
   private final String indexedQuery = String.format("From %s where name : 'Donald'", Person.class.getName());
   private final String nonIndexedQuery = String.format("From %s where nonIndexedField = 'first'", Person.class.getName());
   private final String hybridQuery = String.format("From %s where nonIndexedField = 'first' and age > 50", Person.class.getName());

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheCfg.statistics().enable();
      cacheCfg.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class)
            .addIndexedEntity(Transaction.class);

      createClusteredCaches(3, QueryTestSCI.INSTANCE, cacheCfg);

      cache0 = cache(0);
      cache1 = cache(1);
      cache2 = cache(2);
      queryStatistics0 = Search.getSearchStatistics(cache0).getQueryStatistics();
      queryStatistics1 = Search.getSearchStatistics(cache1).getQueryStatistics();
      queryStatistics2 = Search.getSearchStatistics(cache2).getQueryStatistics();
   }

   @BeforeMethod
   public void setUp() {
      cache0.clear();
   }

   @Test
   public void testQueryStats() {
      addData();

      testNonIndexedQueryStats();
      testIndexedQueryStats();
      testHybridQueryStats();
      testClean();
   }

   @Test
   public void testEmptyIndexStats() {
      Set<String> expectedEntities = new HashSet<>(Arrays.asList(Person.class.getName(), Transaction.class.getName()));

      Set<String> totalEntities = new HashSet<>();
      for (int i = 0; i < cacheManagers.size(); i++) {
         SearchStatistics searchStatistics = Search.getSearchStatistics(cache(i));
         Map<String, IndexInfo> indexInfos = await(searchStatistics.getIndexStatistics().computeIndexInfos());
         totalEntities.addAll(indexInfos.keySet());
         for (IndexInfo indexInfo : indexInfos.values()) {
            assertEquals(0L, indexInfo.count());
            assertThat(indexInfo.size()).isLessThan(MAX_EMPTY_INDEX_SIZE);
         }
      }
      assertEquals(totalEntities, expectedEntities);

      SearchStatistics clusteredStats = await(Search.getClusteredSearchStatistics(cache0));
      Map<String, IndexInfo> classIndexInfoMap = await(clusteredStats.getIndexStatistics().computeIndexInfos());
      assertEquals(classIndexInfoMap.keySet(), expectedEntities);

      Long reduceCount = classIndexInfoMap.values().stream().map(IndexInfo::count).reduce(0L, Long::sum);
      assertEquals(0L, reduceCount.intValue());

      Long reduceSize = classIndexInfoMap.values().stream().map(IndexInfo::size).reduce(0L, Long::sum);
      assertThat(reduceSize.longValue()).isLessThan(MAX_EMPTY_INDEX_SIZE * 2); // 2 indexes
   }

   @Test
   public void testNonEmptyIndexStats() {
      addData();

      Set<String> expectedEntities = new HashSet<>(Arrays.asList(Person.class.getName(), Transaction.class.getName()));
      int expectDocuments = cacheManagers.size() * cache0.getCacheConfiguration().clustering().hash().numOwners();
      flushAll();
      Set<String> totalEntities = new HashSet<>();
      int totalCount = 0;
      long totalSize = 0L;
      for (int i = 0; i < cacheManagers.size(); i++) {
         SearchStatistics searchStatistics = Search.getSearchStatistics(cache(i));
         Map<String, IndexInfo> indexInfos = await(searchStatistics.getIndexStatistics().computeIndexInfos());
         totalEntities.addAll(indexInfos.keySet());
         for (IndexInfo indexInfo : indexInfos.values()) {
            totalCount += indexInfo.count();
            totalSize += indexInfo.size();
         }
      }
      assertEquals(totalEntities, expectedEntities);
      assertEquals(totalCount, expectDocuments);
      assertEquals(totalSize, totalIndexSize());

      SearchStatistics clusteredStats = await(Search.getClusteredSearchStatistics(cache0));
      Map<String, IndexInfo> classIndexInfoMap = await(clusteredStats.getIndexStatistics().computeIndexInfos());
      assertEquals(classIndexInfoMap.keySet(), expectedEntities);

      Long reduceCount = classIndexInfoMap.values().stream().map(IndexInfo::count).reduce(0L, Long::sum);
      int clusteredExpectDocuments = cacheManagers.size() * cache0.getCacheConfiguration().clustering().hash().numOwners();
      assertEquals(reduceCount.intValue(), clusteredExpectDocuments);

      Long reduceSize = classIndexInfoMap.values().stream().map(IndexInfo::size).reduce(0L, Long::sum);
      assertEquals(reduceSize.longValue(), totalIndexSize());
   }

   private void testClean() {
      queryStatistics0.clear();
      queryStatistics1.clear();
      queryStatistics2.clear();

      SearchStatistics clustered = await(Search.getClusteredSearchStatistics(cache0));
      QueryStatistics localQueryStatistics = clustered.getQueryStatistics();
      assertEquals(0, localQueryStatistics.getNonIndexedQueryCount());
      assertEquals(0, localQueryStatistics.getHybridQueryCount());
      assertEquals(0, localQueryStatistics.getDistributedIndexedQueryCount());
      assertEquals(0, localQueryStatistics.getLocalIndexedQueryCount());
   }

   private void testNonIndexedQueryStats() {
      executeQuery(nonIndexedQuery, cache0);

      assertEquals(1, queryStatistics0.getNonIndexedQueryCount());
      assertEquals(0, queryStatistics1.getNonIndexedQueryCount());
      assertEquals(0, queryStatistics2.getNonIndexedQueryCount());
      SearchStatistics clustered1 = await(Search.getClusteredSearchStatistics(cache1));
      assertEquals(1, clustered1.getQueryStatistics().getNonIndexedQueryCount());

      executeQuery(nonIndexedQuery, cache1);

      assertEquals(1, queryStatistics0.getNonIndexedQueryCount());
      assertEquals(1, queryStatistics1.getNonIndexedQueryCount());
      assertEquals(0, queryStatistics2.getNonIndexedQueryCount());
      SearchStatistics clustered2 = await(Search.getClusteredSearchStatistics(cache2));
      assertEquals(2, clustered2.getQueryStatistics().getNonIndexedQueryCount());

      executeQuery(nonIndexedQuery, cache2);

      assertEquals(1, queryStatistics0.getNonIndexedQueryCount());
      assertEquals(1, queryStatistics1.getNonIndexedQueryCount());
      assertEquals(1, queryStatistics2.getNonIndexedQueryCount());
      SearchStatistics clustered0 = await(Search.getClusteredSearchStatistics(cache0));
      assertEquals(3, clustered0.getQueryStatistics().getNonIndexedQueryCount());
   }

   private void testIndexedQueryStats() {
      executeQuery(indexedQuery, cache0);

      assertEquals(1, queryStatistics0.getLocalIndexedQueryCount());
      assertEquals(1, queryStatistics0.getDistributedIndexedQueryCount());

      assertEquals(1, queryStatistics1.getLocalIndexedQueryCount());
      assertEquals(0, queryStatistics1.getDistributedIndexedQueryCount());

      assertEquals(1, queryStatistics2.getLocalIndexedQueryCount());
      assertEquals(0, queryStatistics2.getDistributedIndexedQueryCount());

      SearchStatistics clustered = await(Search.getClusteredSearchStatistics(cache1));
      assertEquals(3, clustered.getQueryStatistics().getLocalIndexedQueryCount());
      assertEquals(1, clustered.getQueryStatistics().getDistributedIndexedQueryCount());

      executeQuery(indexedQuery, cache1);

      assertEquals(2, queryStatistics0.getLocalIndexedQueryCount());
      assertEquals(1, queryStatistics0.getDistributedIndexedQueryCount());

      assertEquals(2, queryStatistics1.getLocalIndexedQueryCount());
      assertEquals(1, queryStatistics1.getDistributedIndexedQueryCount());

      assertEquals(2, queryStatistics2.getLocalIndexedQueryCount());
      assertEquals(0, queryStatistics2.getDistributedIndexedQueryCount());

      clustered = await(Search.getClusteredSearchStatistics(cache1));
      assertEquals(6, clustered.getQueryStatistics().getLocalIndexedQueryCount());
      assertEquals(2, clustered.getQueryStatistics().getDistributedIndexedQueryCount());

      executeQuery(indexedQuery, cache2);

      assertEquals(3, queryStatistics0.getLocalIndexedQueryCount());
      assertEquals(1, queryStatistics0.getDistributedIndexedQueryCount());

      assertEquals(3, queryStatistics1.getLocalIndexedQueryCount());
      assertEquals(1, queryStatistics1.getDistributedIndexedQueryCount());

      assertEquals(3, queryStatistics2.getLocalIndexedQueryCount());
      assertEquals(1, queryStatistics2.getDistributedIndexedQueryCount());

      clustered = await(Search.getClusteredSearchStatistics(cache1));
      assertEquals(9, clustered.getQueryStatistics().getLocalIndexedQueryCount());
      assertEquals(3, clustered.getQueryStatistics().getDistributedIndexedQueryCount());
   }

   private void testHybridQueryStats() {
      executeQuery(hybridQuery, cache0);

      assertEquals(1, queryStatistics0.getHybridQueryCount());
      assertEquals(4, queryStatistics0.getLocalIndexedQueryCount());
      assertEquals(2, queryStatistics0.getDistributedIndexedQueryCount());

      assertEquals(0, queryStatistics1.getHybridQueryCount());
      assertEquals(4, queryStatistics1.getLocalIndexedQueryCount());
      assertEquals(1, queryStatistics1.getDistributedIndexedQueryCount());

      assertEquals(0, queryStatistics2.getHybridQueryCount());
      assertEquals(4, queryStatistics2.getLocalIndexedQueryCount());
      assertEquals(1, queryStatistics2.getDistributedIndexedQueryCount());

      SearchStatistics clustered = await(Search.getClusteredSearchStatistics(cache1));
      assertEquals(1, clustered.getQueryStatistics().getHybridQueryCount());
      assertEquals(12, clustered.getQueryStatistics().getLocalIndexedQueryCount());
      assertEquals(4, clustered.getQueryStatistics().getDistributedIndexedQueryCount());

      executeQuery(hybridQuery, cache1);

      assertEquals(1, queryStatistics0.getHybridQueryCount());
      assertEquals(5, queryStatistics0.getLocalIndexedQueryCount());
      assertEquals(2, queryStatistics0.getDistributedIndexedQueryCount());

      assertEquals(1, queryStatistics1.getHybridQueryCount());
      assertEquals(5, queryStatistics1.getLocalIndexedQueryCount());
      assertEquals(2, queryStatistics1.getDistributedIndexedQueryCount());

      assertEquals(0, queryStatistics2.getHybridQueryCount());
      assertEquals(5, queryStatistics2.getLocalIndexedQueryCount());
      assertEquals(1, queryStatistics2.getDistributedIndexedQueryCount());

      clustered = await(Search.getClusteredSearchStatistics(cache1));
      assertEquals(2, clustered.getQueryStatistics().getHybridQueryCount());
      assertEquals(15, clustered.getQueryStatistics().getLocalIndexedQueryCount());
      assertEquals(5, clustered.getQueryStatistics().getDistributedIndexedQueryCount());

      executeQuery(hybridQuery, cache2);

      assertEquals(1, queryStatistics0.getHybridQueryCount());
      assertEquals(6, queryStatistics0.getLocalIndexedQueryCount());
      assertEquals(2, queryStatistics0.getDistributedIndexedQueryCount());

      assertEquals(1, queryStatistics1.getHybridQueryCount());
      assertEquals(6, queryStatistics1.getLocalIndexedQueryCount());
      assertEquals(2, queryStatistics1.getDistributedIndexedQueryCount());

      assertEquals(1, queryStatistics2.getHybridQueryCount());
      assertEquals(6, queryStatistics2.getLocalIndexedQueryCount());
      assertEquals(2, queryStatistics2.getDistributedIndexedQueryCount());

      clustered = await(Search.getClusteredSearchStatistics(cache1));
      assertEquals(3, clustered.getQueryStatistics().getHybridQueryCount());
      assertEquals(18, clustered.getQueryStatistics().getLocalIndexedQueryCount());
      assertEquals(6, clustered.getQueryStatistics().getDistributedIndexedQueryCount());
   }

   private void executeQuery(String q, Cache<String, Object> fromCache) {
      List<Person> list = fromCache.<Person>query(q).execute().list();
      assertFalse(list.isEmpty());
   }

   private void addData() {
      Person person1 = new Person("Donald", "Duck", 86);
      person1.setNonIndexedField("second");
      Person person2 = new Person("Mickey", "Mouse", 92);
      person2.setNonIndexedField("first");
      cache0.put("1", person1);
      cache0.put("2", person2);
      cache0.put("3", new Transaction(12, "sss"));
   }

   private void flushAll() {
      caches().forEach(c -> ComponentRegistryUtils.getSearchMapping(c).scopeAll().workspace().flush());
   }

   private long totalIndexSize() {
      long totalSize = 0;
      for (Cache<?, ?> cache : caches()) {
         SearchMapping searchMapping = ComponentRegistryUtils.getSearchMapping(cache);
         for (SearchIndexedEntity indexedEntity : searchMapping.allIndexedEntities()) {
            totalSize += indexedEntity.indexManager().unwrap(LuceneIndexManager.class).computeSizeInBytes();
         }
      }
      return totalSize;
   }
}
