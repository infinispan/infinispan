package org.infinispan.query.maxresult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.Cache;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.model.Game;
import org.infinispan.query.model.NonIndexedGame;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.maxresult.InfinispanDefaultMaxResultTest")
@TestForIssue(jiraKey = "ISPN-14194")
public class EmbeddedDefaultMaxResultTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("org.infinispan.query.model.Game");

      ConfigurationBuilder notIndexed = new ConfigurationBuilder();

      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager();
      manager.defineConfiguration("indexed-games", indexed.build());
      manager.defineConfiguration("not-indexed-games", notIndexed.build());
      return manager;
   }

   @Test
   public void testNonIndexed() {
      Cache<Integer, NonIndexedGame> games = cacheManager.getCache("not-indexed-games");

      for (int i = 1; i <= 110; i++) {
         games.put(i, new NonIndexedGame("Game " + i, "This is the game " + i + "# of a series"));
      }

      QueryFactory factory = Search.getQueryFactory(games);
      Query<NonIndexedGame> query = factory.create("from org.infinispan.query.model.NonIndexedGame");
      QueryResult<NonIndexedGame> result = query.execute();

      assertThat(result.hitCount()).hasValue(110);
      assertThat(result.list()).hasSize(100); // use the default

      query = factory.create("from org.infinispan.query.model.NonIndexedGame");
      query.maxResults(200); // raise it
      result = query.execute();

      assertThat(result.hitCount()).hasValue(110);
      assertThat(result.list()).hasSize(110);
   }

   @Test
   public void testIndexed() {
      Cache<Integer, Game> games = cacheManager.getCache("indexed-games");

      for (int i = 1; i <= 110; i++) {
         games.put(i, new Game("Game " + i, "This is the game " + i + "# of a series"));
      }

      QueryFactory factory = Search.getQueryFactory(games);
      Query<Game> query = factory.create("from org.infinispan.query.model.Game");
      QueryResult<Game> result = query.execute();

      assertThat(result.hitCount()).hasValue(110);
      assertThat(result.list()).hasSize(100); // use the default

      query = factory.create("from org.infinispan.query.model.Game");
      query.maxResults(200); // raise it
      result = query.execute();

      assertThat(result.hitCount()).hasValue(110);
      assertThat(result.list()).hasSize(110);
   }
}
