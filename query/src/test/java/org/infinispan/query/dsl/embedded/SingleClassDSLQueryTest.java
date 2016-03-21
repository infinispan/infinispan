package org.infinispan.query.dsl.embedded;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * @author gustavonalle
 * @author Tristan Tarrant
 * @since 8.0
 */
@Test(groups = "functional", testName = "query.dsl.embedded.SingleClassDSLQueryTest")
public class SingleClassDSLQueryTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().index(Index.ALL)
              .addIndexedEntity(Person.class)
              .addProperty("default.directory_provider", "ram")
              .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   /**
    * Test querying for entities defined as inner classes.
    */
   public void testQuery() throws Exception {
      Cache<String, Person> cache = cacheManager.getCache();
      cache.put("person1", new Person("William", "Shakespeare"));
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.from(Person.class).having("name").eq("William").toBuilder().build();
      List<Person> matches = query.list();
      assertEquals(1, matches.size());
   }

   @Indexed
   static class Person {
      @Field(store = Store.YES, analyze = Analyze.NO)
      String name;

      @Field(store = Store.YES, analyze = Analyze.NO, indexNullAs = Field.DEFAULT_NULL_TOKEN)
      String surname;

      public Person(String name, String surname) {
         this.name = name;
         this.surname = surname;
      }

   }
}
