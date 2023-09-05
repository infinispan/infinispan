package org.infinispan.query.maxresult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.model.Game;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.maxresult.DistributedHitCountAccuracyTest")
@TestForIssue(jiraKey = "ISPN-15036")
public class DistributedHitCountAccuracyTest extends MultipleCacheManagersTest {

   private static final String QUERY_TEXT = "from org.infinispan.query.model.Game where description : 'game'";
   private Cache<Integer, Game> node1;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      config
            .clustering().hash().numOwners(2)
            .indexing().enable()
               .storage(LOCAL_HEAP)
               .addIndexedEntity("org.infinispan.query.model.Game")
            .query().hitCountAccuracy(10); // lower the default accuracy;

      createClusteredCaches(2, config);
      node1 = cache(0);
      cache(1);
   }

   @Test
   public void smokeTest() {
      executeSmokeTest(node1);
   }

   static void executeSmokeTest(Cache<Integer, Game> cache) {
      for (int i = 1; i <= 5000; i++) {
         cache.put(i, new Game("Game " + i, "This is the game " + i + "# of a series"));
      }

      Query<Game> query = cache.query(QUERY_TEXT);
      QueryResult<Game> result = query.execute();

      assertThat(result.list()).hasSize(100);
      // the hit count accuracy does not allow to compute **an exact** hit count
      assertThat(result.count().isExact()).isFalse();

      query = cache.query(QUERY_TEXT);
      // raise the default accuracy
      query.hitCountAccuracy(5_000);
      result = query.execute();

      assertThat(result.list()).hasSize(100);
      assertThat(result.count().isExact()).isTrue();
      assertThat(result.count().value()).isEqualTo(5_000);

      // the distributed iterator is supposed to work normally
      query = cache.query(QUERY_TEXT);
      try (CloseableIterator<Game> iterator = query.iterator()) {
         assertThat(iterator).toIterable().hasSize(100);
      }

      query = cache.query(QUERY_TEXT);
      // raise the default accuracy
      query.hitCountAccuracy(5_000);
      try (CloseableIterator<Game> iterator = query.iterator()) {
         assertThat(iterator).toIterable().hasSize(100);
      }
   }
}
