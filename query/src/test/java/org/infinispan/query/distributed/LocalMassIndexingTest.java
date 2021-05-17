package org.infinispan.query.distributed;

import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Indexer;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.query.core.stats.SearchStatistics;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests for selectively reindexing members of the clusters by using local mode
 *
 * @since 13.0
 */
@Test(groups = "functional", testName = "query.distributed.LocalFlagMassIndexingTest")
public class LocalMassIndexingTest extends MultipleCacheManagersTest {

   protected static final int NUM_NODES = 3;
   private static final int ENTRIES = 50;

   protected String getConfigurationFile() {
      return "dynamic-indexing-distribution.xml";
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < NUM_NODES; i++) {
         EmbeddedCacheManager cacheManager = TestCacheManagerFactory.fromXml(getConfigurationFile());
         registerCacheManager(cacheManager);
         cacheManager.getCache();
      }
      waitForClusterToForm();
      Cache<Integer, Car> cache = cache(0);
      IntStream.range(0, ENTRIES).forEach(i -> cache.put(i, new Car("brand", "color", 100)));
   }

   public void testReindexing() throws Exception {
      final Indexer indexer0 = Search.getIndexer(cache(0));
      final Indexer indexer1 = Search.getIndexer(cache(1));
      final Indexer indexer2 = Search.getIndexer(cache(2));

      join(indexer0.run());
      assertAllIndexed();

      clearIndexes();

      // Local indexing should not touch the indexes of other caches
      join(indexer0.runLocal());
      assertOnlyIndexed(0);

      clearIndexes();

      join(indexer1.runLocal());
      assertOnlyIndexed(1);

      clearIndexes();

      join(indexer2.runLocal());
      assertOnlyIndexed(2);
   }

   void clearIndexes() {
      join(Search.getIndexer(cache(0)).remove());
   }

   private void assertIndexState(BiConsumer<IndexInfo, Integer> cacheIndexInfo) {
      IntStream.range(0, NUM_NODES).forEach(i -> {
         Cache<?, ?> cache = cache(i);
         SearchStatistics searchStatistics = Search.getSearchStatistics(cache);
         Map<String, IndexInfo> indexInfo = join(searchStatistics.getIndexStatistics().computeIndexInfos());
         cacheIndexInfo.accept(indexInfo.get(Car.class.getName()), i);
      });
   }

   private void assertAllIndexed() {
      assertIndexState((indexInfo, i) -> assertTrue(indexInfo.count() > 0));
   }

   private void assertOnlyIndexed(int id) {
      assertIndexState((indexInfo, i) -> {
         long count = indexInfo.count();
         if (i == id) {
            assertTrue(count > 0);
         } else {
            assertEquals(count, 0);
         }
      });
   }

}
