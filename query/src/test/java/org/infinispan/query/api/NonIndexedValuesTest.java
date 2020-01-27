package org.infinispan.query.api;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.api.NonIndexedValuesTest")
public class NonIndexedValuesTest extends SingleCacheManagerTest {

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(true);
      c.indexing()
         .enable()
         .addIndexedEntity(TestEntity.class)
         .addIndexedEntity(AnotherTestEntity.class)
         .addProperty("default.directory_provider", "local-heap")
         .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
         .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, c);
   }

   private Query createISPNQuery(Class<?> entity) {
      return createQuery(entity, "ISPN-1949");
   }

   private Query createHSearchQuery(Class<?> entity) {
      return createQuery(entity, "HSEARCH-1077");
   }

   private Query createQuery(Class<?> entity, String issueValue) {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      String queryStr = "FROM %s WHERE name = '%s'";
      return queryFactory.create(String.format(queryStr, entity.getName(), issueValue));
   }

   @Test
   public void testReplaceSimpleSearchable() {
      TestEntity se1 = new TestEntity("ISPN-1949", "Allow non-indexed values in indexed caches", 10, "note");
      cache.put(se1.getId(), se1);
      cache.put("name2", "some string value");
      cache.put("name3", "some string value");
      cache.put("name3", new NotIndexedType("some string value"));

      assertEquals(1, createISPNQuery(TestEntity.class).list().size());
      cache.put(se1.getId(), "some string value");

      assertEquals(0, createISPNQuery(TestEntity.class).list().size());

      AnotherTestEntity indexBEntity = new AnotherTestEntity("ISPN-1949");
      cache.put("name", indexBEntity);
      assertEquals(0, createISPNQuery(TestEntity.class).list().size());
      assertEquals(1, createISPNQuery(AnotherTestEntity.class).list().size());

      TestEntity se2 = new TestEntity("HSEARCH-1077", "Mutable SearchFactory should return which classes are actually going to be indexed", 10, "note");
      cache.replace("name", indexBEntity, se2);
      assertEquals(0, createISPNQuery(TestEntity.class).list().size());
      assertEquals(1, createHSearchQuery(TestEntity.class).list().size());

      //a failing atomic replace should not change the index:
      cache.replace("name", "notMatching", "notImportant");
      assertEquals(1, createHSearchQuery(TestEntity.class).list().size());
      assertEquals(0, createISPNQuery(TestEntity.class).list().size());

      cache.remove("name");
      assertEquals(0, createHSearchQuery(TestEntity.class).list().size());

      cache.put("name", se2);
      assertEquals(1, createHSearchQuery(TestEntity.class).list().size());
      cache.put("name", "replacement String");
      assertEquals(0, createHSearchQuery(TestEntity.class).list().size());

      cache.put("name", se1);
      assertEquals(1, createISPNQuery(TestEntity.class).list().size());
      cache.put("second name", se1);
      assertEquals(2, createISPNQuery(TestEntity.class).list().size());
      assertEquals(0, createISPNQuery(AnotherTestEntity.class).list().size());

      //now actually replace one with a different indexed type (matches same query)
      cache.replace("name", se1, indexBEntity);
      assertEquals(1, createISPNQuery(TestEntity.class).list().size());
      assertEquals(1, createISPNQuery(AnotherTestEntity.class).list().size());

      //replace with a non indexed type
      cache.replace("name", indexBEntity, new NotIndexedType("this is not indexed"));
      assertEquals(1, createISPNQuery(TestEntity.class).list().size());

      StaticTestingErrorHandler.assertAllGood(cache);
   }

}
