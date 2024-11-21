package org.infinispan.client.hotrod.query.embedded;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;

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

   @DataProvider
   static Object[][] queryOffsetBatchExpected() {
      return new Object[][] {
            // Only a single elements is requested (maxResults = 1) which therefore will always require a single
            // query per publisher. Note that we run 2 queries in the test so the second to last field is always
            // double what the value is per query.
            {1, 0, 1, 2, 1},
            // Since batch size is half the max results we need two calls to fill max results per publisher
            {1, 0, 2, 4, 2},
            {1, 1, 2, 4, 2},
            // Due to batch size being equal to 1 we need 5 queries when four elements are present. One for each value and
            // a last for the empty result
            {1, 0, 10, 10, 5},
            {1, 0, -1, 10, 5},

            {2, 0, 1, 2, 1},
            {2, 0, 2, 2, 1},
            // Batch size was half the amount of entries so we need 2 queries to get all values then an additional
            // one to see nothing else is left per publisher
            {2, 0, 10, 6, 3},
            // Note this one has only 4 queries because the offset has made it so that a batch is no longer filled
            // so we don't have the empty query additional
            {2, 1, 10, 4, 2},
            {2, 0, -1, 6, 3},

            {4, 0, 1, 2, 1},
            {4, 2, 1, 2, 1},
            {4, 0, 2, 2, 1},
            // When the batch size is exactly the same size as results we need 2 queries, one with all values and
            // another that is empty
            {4, 0, 10, 4, 2},
            {4, 0, -1, 4, 2},

            // Remaining the batch size is larger than result size so we only need 1 query call per publisher
            {10, 0, 1, 2, 1},
            {10, 0, 2, 2, 1},
            {10, 0, 10, 2, 1},
            {10, 0, -1, 2, 1},

            {Integer.MAX_VALUE, 0, 1, 2, 1},
            {Integer.MAX_VALUE, 0, 2, 2, 1},
            {Integer.MAX_VALUE, 0, 10, 2, 1},
            {Integer.MAX_VALUE, 0, -1, 2, 1},
      };
   }

   @Test(dataProvider = "queryOffsetBatchExpected")
   public void remoteQueriesReactive(int batchSize, int initialOffset, int maxReturn, int expectedIndexQueries, int expectedHybridQueries) {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();

      remoteCache.put(3, new Game("Myst",
            "Myst is a 1993 point-and-click puzzle adventure game developed and published by Cyan, Inc."));
      cache.put(4, new Game("Riven",
            "Myst is a 1997 point-and-click puzzle adventure game developed by Cyan Worlds and published by Red Orb Entertainment."));

      List<String> names = cache.values().stream()
            .map(g -> ((Game) g).getName())
            .sorted()
            // Can't use toList until https://issues.redhat.com/browse/ISPN-16341 is integrated
            .collect(Collectors.toList());

      Query<Game> query = remoteCache.query(QUERY_TEXT + " ORDER BY name");
      query.startOffset(initialOffset);
      query.maxResults(maxReturn);
      List<Game> list = Flowable.fromPublisher(query.publish(batchSize))
            .toList()
            .blockingGet();
      switch (maxReturn) {
         case 1 -> assertThat(list).extracting("name")
               .containsExactly(names.get(initialOffset));
         case 2 -> assertThat(list).extracting("name")
               .containsExactly(names.get(initialOffset), names.get(initialOffset + 1));
         default -> {
            List<String> expectedList = Arrays.asList("Monkey Island 2: LeChuck's Revenge", "Myst", "Riven", "The Secret of Monkey Island");
            assertThat(list).extracting("name")
                  .containsExactly(expectedList.subList(initialOffset, expectedList.size()).toArray());
         }
      }

      // this query is hybrid, since the fields name and description are not projectable
      Query<Object[]> queryProj = remoteCache.query(QUERY_PROJ_TEXT + " ORDER BY name");
      queryProj.startOffset(initialOffset);
      queryProj.maxResults(maxReturn);
      List<Object[]> proj = Flowable.fromPublisher(queryProj.publish(batchSize))
            .toList()
            .blockingGet();
      switch (maxReturn) {
         case 1 -> assertThat(proj).extracting(objects -> objects[0])
               .containsExactly(names.get(initialOffset));
         case 2 -> assertThat(proj).extracting(objects -> objects[0])
               .containsExactly(names.get(initialOffset), names.get(initialOffset + 1));
         default -> {
            List<String> expectedList = Arrays.asList("Monkey Island 2: LeChuck's Revenge", "Myst", "Riven", "The Secret of Monkey Island");
            assertThat(proj).extracting(objects -> objects[0])
                  .containsExactly(expectedList.subList(initialOffset, expectedList.size()).toArray());
         }
      }
      expectedIndexedQueries(expectedIndexQueries, expectedHybridQueries);
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

   @AfterMethod
   public void resetStats() {
      QueryStatistics queryStatistics = Search.getSearchStatistics(cache).getQueryStatistics();
      queryStatistics.clear();
   }

   private void expectedIndexedQueries(int expectedIndexedQueries, int expectedHybridQueries) {
      QueryStatistics queryStatistics = Search.getSearchStatistics(cache).getQueryStatistics();
      assertThat(queryStatistics.getLocalIndexedQueryCount()).isEqualTo(expectedIndexedQueries);
      assertThat(queryStatistics.getHybridQueryCount()).isEqualTo(expectedHybridQueries);
      assertThat(queryStatistics.getNonIndexedQueryCount()).isZero();
   }
}
