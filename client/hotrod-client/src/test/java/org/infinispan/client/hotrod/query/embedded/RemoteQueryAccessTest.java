package org.infinispan.client.hotrod.query.embedded;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.QueryStatistics;
import org.infinispan.query.model.Game;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.tasks.query.RemoteQueryAccess;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.embedded.RemoteQueryAccessTest")
public class RemoteQueryAccessTest extends SingleHotRodServerTest {

   private static final String QUERY_TEXT = "from Game where description : 'adventure'";
   private static final String QUERY_PROJ_TEXT = "select name, description " + QUERY_TEXT;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.statistics().enable();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("Game");

      return TestCacheManagerFactory.createServerModeCacheManager(indexed);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Game.GameSchema.INSTANCE;
   }

   @BeforeMethod
   public void setUp() {
      Search.getSearchStatistics(cache).getQueryStatistics().clear();

      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      if (!remoteCache.isEmpty()) {
         return;
      }

      // using remote and embedded API to test the interoperability
      remoteCache.put(1, new Game("The Secret of Monkey Island",
            "The Secret of Monkey Island is a 1990 point-and-click graphic adventure game developed and published by Lucasfilm Games."));
      cache.put(2, new Game("Monkey Island 2: LeChuck's Revenge",
            "Monkey Island 2: LeChuck's Revenge is an adventure game developed and published by LucasArts in 1991."));
   }

   @Test
   public void remoteQueries() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      Query<Game> query = remoteCache.query(QUERY_TEXT);
      List<Game> list = query.list();
      assertThat(list).extracting("name")
            .containsExactlyInAnyOrder("The Secret of Monkey Island", "Monkey Island 2: LeChuck's Revenge");

      // this query is hybrid, since the fields name and description are not projectable
      Query<Object[]> queryProj = remoteCache.query(QUERY_PROJ_TEXT);
      List<Object[]> proj = queryProj.list();
      assertThat(proj).extracting(objects -> objects[0])
            .containsExactlyInAnyOrder("The Secret of Monkey Island", "Monkey Island 2: LeChuck's Revenge");

      expectedIndexedQueries(2, 1);
   }

   @Test
   public void remoteQueriesFromEmbedded() {
      RemoteQueryAccess remoteQueryAccess = SecurityActions
            .getCacheComponentRegistry(cache.getAdvancedCache()).getComponent(RemoteQueryAccess.class);

      Query<Game> query = remoteQueryAccess.query(QUERY_TEXT);
      List<Game> list = query.list();
      assertThat(list).extracting("name")
            .containsExactlyInAnyOrder("The Secret of Monkey Island", "Monkey Island 2: LeChuck's Revenge");

      // this query is hybrid, since the fields name and description are not projectable
      Query<Object[]> queryProj = remoteQueryAccess.query(QUERY_PROJ_TEXT);
      List<Object[]> proj = queryProj.list();
      assertThat(proj).extracting(objects -> objects[0])
            .containsExactlyInAnyOrder("The Secret of Monkey Island", "Monkey Island 2: LeChuck's Revenge");

      expectedIndexedQueries(2, 1);
   }

   private void expectedIndexedQueries(int expectedIndexedQueries, int expectedHybridQueries) {
      QueryStatistics queryStatistics = Search.getSearchStatistics(cache).getQueryStatistics();
      assertThat(queryStatistics.getLocalIndexedQueryCount()).isEqualTo(expectedIndexedQueries);
      assertThat(queryStatistics.getHybridQueryCount()).isEqualTo(expectedHybridQueries);
      assertThat(queryStatistics.getNonIndexedQueryCount()).isZero();
   }
}
