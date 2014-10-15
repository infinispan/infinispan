package org.infinispan.all.embeddedquery;

import static org.junit.Assert.*;

import org.apache.lucene.search.Query;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.junit.Test;

public class EmbeddedAllQueryTest {

   @Test
   public void testAllEmbedded() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
         .indexing()
            .index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      EmbeddedCacheManager cm = new DefaultCacheManager(builder.build());

      Cache<Long, TestEntity> cache = cm.getCache();
      cache.put(1l, new TestEntity("Adam", "Smith", 1l, "A note about Adam"));
      cache.put(2l, new TestEntity("Eve", "Smith", 2l, "A note about Eve"));
      cache.put(3l, new TestEntity("Abel", "Smith", 3l, "A note about Abel"));
      cache.put(4l, new TestEntity("Cain", "Smith", 4l, "A note about Cain"));

      SearchManager sm = Search.getSearchManager(cache);
      Query query = sm.buildQueryBuilderForClass(TestEntity.class)
            .get().keyword().onField("name").matching("Eve").createQuery();
      CacheQuery q1 = sm.getQuery(query);
      assertEquals(1, q1.getResultSize());
      assertEquals(TestEntity.class, q1.list().get(0).getClass());
      cm.stop();
   }
}
