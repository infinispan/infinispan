package org.infinispan.query.maxresult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.model.Game;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.maxresult.DistributedMaxResultTest")
public class DistributedMaxResultTest extends MultipleCacheManagersTest {

   private Cache<Integer, Game> node1;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      config
            .clustering()
               .hash().numOwners(2)
               .stateTransfer().chunkSize(100)
            .indexing().enable()
               .storage(LOCAL_HEAP)
               .addIndexedEntity("org.infinispan.query.model.Game")
            .query().defaultMaxResults(50);

      createClusteredCaches(2, config);
      node1 = cache(0);
      cache(1);
   }

   public void executeSmokeTest() {
      for (int i = 1; i <= 110; i++) {
         node1.put(i, new Game("Game " + i, "This is the game " + i + "# of a series"));
      }

      Query<Game> query = node1.query("from org.infinispan.query.model.Game");
      QueryResult<Game> result = query.execute();

      assertThat(result.count().value()).isEqualTo(110);
      assertThat(result.list()).hasSize(50); // use custom default

      query = node1.query("from org.infinispan.query.model.Game");
      query.maxResults(200); // raise it
      result = query.execute();

      assertThat(result.count().value()).isEqualTo(110);
      assertThat(result.list()).hasSize(110);
   }
}
