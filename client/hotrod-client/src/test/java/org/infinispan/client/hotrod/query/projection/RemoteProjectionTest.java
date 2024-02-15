package org.infinispan.client.hotrod.query.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.model.Game;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.projection.RemoteProjectionTest")
public class RemoteProjectionTest extends SingleHotRodServerTest {

   private static final int ENTRIES = 10;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("Game");
      return TestCacheManagerFactory.createServerModeCacheManager(indexed);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Game.GameSchema.INSTANCE;
   }

   @Test
   public void test() {
      RemoteCache<String, Game> games = remoteCacheManager.getCache();
      for (int i = 1; i <= ENTRIES; i++) {
         games.put("g" + i, new Game("bla" + i, "bla bla" + i));
      }

      QueryFactory queryFactory = Search.getQueryFactory(games);

      Query<Object[]> query;
      QueryResult<Object[]> result;

      query = queryFactory.create("select g.description from Game g where g.description : 'bla3'");
      result = query.execute();
      assertThat(result.list()).extracting(array -> array[0]).containsExactly("bla bla3");

      query = queryFactory.create("select g from Game g where g.description : 'bla3'");
      result = query.execute();
      assertThat(result.list()).extracting(array -> array[0]).extracting("name").containsExactly("bla3");

      query = queryFactory.create("select g, g.description from Game g where g.description : 'bla3'");
      result = query.execute();
      assertThat(result.list()).extracting(array -> array[0]).extracting("name").containsExactly("bla3");
      assertThat(result.list()).extracting(array -> array[1]).containsExactly("bla bla3");
   }
}
