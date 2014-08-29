package org.infinispan.query.api;

import static org.junit.Assert.assertEquals;

import org.apache.lucene.search.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.api.NonIndexedValuesTest")
public class NonIndexedValuesTest extends SingleCacheManagerTest {

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(true);
      c.indexing()
         .index(Index.ALL)
         .addProperty("default.directory_provider", "ram")
         .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
         .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(c);
   }

   @Test
   public void testReplaceSimpleSearchable() {
      TestEntity se1 = new TestEntity("ISPN-1949", "Allow non-indexed values in indexed caches", 10, "note");
      cache.put(se1.getId(), se1);
      cache.put("name2", "some string value");
      cache.put("name3", "some string value");
      cache.put("name3", new NotIndexedType("some string value"));

      SearchManager qf = Search.getSearchManager(cache);

      Query ispnIssueQuery = qf.buildQueryBuilderForClass(TestEntity.class)
         .get()
            .keyword()
               .onField("name")
               .ignoreAnalyzer()
               .matching("ISPN-1949")
            .createQuery();

      assertEquals(1, qf.getQuery(ispnIssueQuery).list().size());

      cache.put(se1.getId(), "some string value" );

      assertEquals(0, qf.getQuery(ispnIssueQuery).list().size());

      AnotherTestEntity indexBEntity = new AnotherTestEntity("ISPN-1949");
      cache.put("name", indexBEntity);
      assertEquals(1, qf.getQuery(ispnIssueQuery).list().size());

      TestEntity se2 = new TestEntity("HSEARCH-1077", "Mutable SearchFactory should return which classes are actually going to be indexed", 10, "note");
      cache.replace("name", indexBEntity, se2);
      assertEquals(0, qf.getQuery(ispnIssueQuery).list().size());

      Query searchIssueQuery = qf.buildQueryBuilderForClass(TestEntity.class)
            .get()
               .keyword()
                  .onField("name")
                  .ignoreAnalyzer()
                  .matching("HSEARCH-1077")
               .createQuery();
      assertEquals(1, qf.getQuery(searchIssueQuery).list().size());

      //a failing atomic replace should not change the index:
      cache.replace("name", "notMatching", "notImportant");
      assertEquals(1, qf.getQuery(searchIssueQuery).list().size());
      assertEquals(0, qf.getQuery(ispnIssueQuery).list().size());

      cache.remove("name");
      assertEquals(0, qf.getQuery(searchIssueQuery).list().size());

      cache.put("name", se2);
      assertEquals(1, qf.getQuery(searchIssueQuery).list().size());
      cache.put("name", "replacement String");
      assertEquals(0, qf.getQuery(searchIssueQuery).list().size());

      cache.put("name", se1);
      assertEquals(1, qf.getQuery(ispnIssueQuery).list().size());
      cache.put("second name", se1);
      assertEquals(2, qf.getQuery(ispnIssueQuery).list().size());
      assertEquals(2, qf.getQuery(ispnIssueQuery, TestEntity.class).list().size());

      //now actually replace one with a different indexed type (matches same query)
      cache.replace("name", se1, indexBEntity);
      assertEquals(2, qf.getQuery(ispnIssueQuery).list().size());
      assertEquals(1, qf.getQuery(ispnIssueQuery, TestEntity.class).list().size());
      assertEquals(1, qf.getQuery(ispnIssueQuery, AnotherTestEntity.class).list().size());

      //replace with a non indexed type
      cache.replace("name", indexBEntity, new NotIndexedType("this is not indexed"));
      assertEquals(1, qf.getQuery(ispnIssueQuery).list().size());

      StaticTestingErrorHandler.assertAllGood(cache);
   }

}
