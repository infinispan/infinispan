package org.infinispan.client.hotrod.annotation.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.assertj.core.api.iterable.Extractor;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.annotation.model.Author;
import org.infinispan.client.hotrod.annotation.model.Poem;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.annotation.test.PoemTest")
public class PoemTest extends SingleHotRodServerTest {

   private static final Extractor<Object[], Object> FIRST_ELEMENT_OF_THE_ARRAY = (item) -> item[0];

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("poem.Poem");

      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager();
      manager.defineConfiguration("poems", builder.build());
      return manager;
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Poem.PoemSchema.INSTANCE;
   }

   @Test
   public void testSearches() {
      RemoteCache<Integer, Poem> remoteCache = remoteCacheManager.getCache("poems");

      remoteCache.put(1, new Poem(new Author("Edgar Allen Poe"), "The Raven", 1845));
      remoteCache.put(2, new Poem(new Author("Emily Dickinson"), "Because I could not stop for Death", 1890));
      remoteCache.put(3, new Poem(new Author("Emma Lazarus"), "The New Colossus", 1883));
      remoteCache.put(4, new Poem(new Author(null), "Alla Sera", null)); // check index as null

      QueryFactory queryFactory = Search.getQueryFactory(remoteCache);

      Query<Object[]> query;
      List<Object[]> poems;

      // basic(searchable) && embedded>keyword(projectable) && embedded>keyword(sortable) && index-as-null check
      // TODO HSEARCH-4584 Restore the original query when it solved:
      //   query = queryFactory.create("select p.author.name from poem.Poem p where p.year < 1885 order by p.author.name");
      query = queryFactory.create("select p.author.name from poem.Poem p order by p.author.name");
      poems = query.execute().list();
      assertThat(poems).extracting(FIRST_ELEMENT_OF_THE_ARRAY)
            .containsExactly("Edgar Allen Poe", "Emily Dickinson", "Emma Lazarus", null);

      assertThat(queryFactory.create("from poem.Poem p where p.year < 1885 order by p.author.name").execute().list())
            .extracting("year").containsExactly(1845, 1883, 1803);

      // text(searchable) && text(projectable) && basic(sortable)
      query = queryFactory.create("select p.description from poem.Poem p where p.description : 'The' order by p.year");
      poems = query.execute().list();
      assertThat(poems).extracting(FIRST_ELEMENT_OF_THE_ARRAY)
            .containsExactly("The Raven", "The New Colossus");

      // whitespace analyzer: the term `The` is not lower cased filtered. So `the` won't match:
      assertThat(queryFactory.create("from poem.Poem p where p.description : 'the'").execute().list()).isEmpty();

      // embedded>keyword(searchable) with lowercase normalizer && basic(projectable)
      query = queryFactory.create("select p.year from poem.Poem p where p.author.name = 'eMMA lazaRUS'");
      poems = query.execute().list();
      assertThat(poems).extracting(FIRST_ELEMENT_OF_THE_ARRAY)
            .containsExactly(1883);
   }
}
