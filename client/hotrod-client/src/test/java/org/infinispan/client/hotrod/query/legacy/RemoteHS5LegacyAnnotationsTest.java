package org.infinispan.client.hotrod.query.legacy;

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
import org.infinispan.query.model.LegacyGame;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.legacy.RemoteHS5LegacyAnnotationsTest")
public class RemoteHS5LegacyAnnotationsTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("LegacyGame");

      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager();
      manager.defineConfiguration("indexed-games", indexed.build());
      return manager;
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return LegacyGame.LegacyGameSchema.INSTANCE;
   }

   @Test
   public void test() {
      RemoteCache<String, LegacyGame> remoteCache = remoteCacheManager.getCache("indexed-games");
      remoteCache.put("a", new LegacyGame("Game A", "bla bla bla bli", 1999));
      remoteCache.put("b", new LegacyGame("Game B", "bla bla bla uuu", 2013));
      remoteCache.put("c", new LegacyGame("Game C", "bla aaa bla bli", 2000));

      QueryFactory factory = Search.getQueryFactory(remoteCache);
      Query<String[]> queryNames =
            // keep using the old API:
            factory.create("select g.name from LegacyGame g where g.name : 'Game B'");
      QueryResult<String[]> resultNames = queryNames.execute();

      assertThat(resultNames.count().isExact()).isTrue();
      assertThat(resultNames.count().value()).isEqualTo(1);
      assertThat(resultNames.list()).containsExactly(new String[]{"Game B"});

      Query<LegacyGame> queryGames =
            // keep using the old API:
            factory.create("from LegacyGame g where g.description : 'bli' order by g.releaseYear desc");
      QueryResult<LegacyGame> resultGames = queryGames.execute();

      assertThat(resultGames.count().isExact()).isTrue();
      assertThat(resultGames.count().value()).isEqualTo(2);
      assertThat(resultGames.list()).extracting("name").containsExactly("Game C", "Game A");
   }
}
