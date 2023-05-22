package org.infinispan.query.maxresult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.model.Game;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

public class HitCountAccuracyTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("org.infinispan.query.model.Game");
      indexed.query().hitCountAccuracy(10); // lower the default accuracy

      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager();
      manager.defineConfiguration("indexed-games", indexed.build());
      return manager;
   }

   @Test
   public void hitCountNotPresent() {
      Cache<Integer, Game> games = cacheManager.getCache("indexed-games");
      for (int i = 1; i <= 5000; i++) {
         games.put(i, new Game("Game " + i, "This is the game " + i + "# of a series"));
      }

      QueryFactory factory = Search.getQueryFactory(games);
      Query<Game> query = factory.create("from org.infinispan.query.model.Game where description : 'game'");
      QueryResult<Game> result = query.execute();

      // the hit count accuracy does not allow to compute the hit count
      assertThat(result.hitCount()).isNotPresent();
   }
}
