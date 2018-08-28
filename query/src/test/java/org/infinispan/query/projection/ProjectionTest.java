package org.infinispan.query.projection;

import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.search.Query;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ProjectionConstants;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.projection.ProjectionTest")
public class ProjectionTest extends SingleCacheManagerTest {

   private SearchManager searchManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.indexing().index(Index.ALL)
            .addIndexedEntity(Foo.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(cfg);
      Cache<Object, Object> cache = cacheManager.getCache();
      searchManager = Search.getSearchManager(cache);
      return cacheManager;
   }

   @Test
   public void testQueryProjectionWithSingleField() {
      cache.put("1", new Foo("bar1", "baz1"));
      CacheQuery<?> cacheQuery = createProjectionQuery("bar");
      assertQueryReturns(cacheQuery, new Object[] { "bar1" });
   }

   @Test
   public void testQueryProjectionWithMultipleFields() {
      cache.put("1", new Foo("bar1", "baz1"));
      CacheQuery<?> cacheQuery = createProjectionQuery("bar", "baz");
      assertQueryReturns(cacheQuery, new Object[] { "bar1", "baz1" });
   }

   @Test
   public void testKeyProjectionConstant() {
      cache.put("1", new Foo("bar1", "baz1"));
      CacheQuery<?> cacheQuery = createProjectionQuery(ProjectionConstants.KEY);
      assertQueryReturns(cacheQuery, new Object[] { "1" });
   }

   @Test
   public void testValueProjectionConstant() {
      Foo foo = new Foo("bar1", "baz1");
      cache.put("1", foo);
      CacheQuery<?> cacheQuery = createProjectionQuery(ProjectionConstants.VALUE);
      assertQueryReturns(cacheQuery, new Object[] { foo });
   }

   @Test
   public void testMixedProjections() {
      Foo foo = new Foo("bar1", "baz4");
      cache.put("1", foo);
      CacheQuery<?> cacheQuery = createProjectionQuery(
            ProjectionConstants.KEY,
            ProjectionConstants.VALUE,
            ProjectionConstants.VALUE,
            org.hibernate.search.engine.ProjectionConstants.OBJECT_CLASS,
            "baz",
            "bar"
            );
      assertQueryReturns(cacheQuery, new Object[] { "1", foo, foo, foo.getClass(), foo.baz, foo.bar });
   }

   private CacheQuery<?> createProjectionQuery(String... projection) {
      QueryBuilder queryBuilder = searchManager.buildQueryBuilderForClass(Foo.class).get();
      Query query = queryBuilder.keyword().onField("bar").matching("bar1").createQuery();
      return searchManager.getQuery(query).projection(projection);
   }

   private void assertQueryReturns(CacheQuery<?> cacheQuery, Object[] expected) {
      assertQueryListContains(cacheQuery.list(), expected);
      try (ResultIterator<?> eagerIterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER))) {
         assertQueryIteratorContains(eagerIterator, expected);
      }
      try (ResultIterator<?> lazyIterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY))) {
         assertQueryIteratorContains(lazyIterator, expected);
      }
   }

   private void assertQueryListContains(List<?> list, Object[] expected) {
      assert list.size() == 1;
      Object[] array = (Object[]) list.get(0);
      assertArrayEquals(expected, array);
   }

   private void assertQueryIteratorContains(ResultIterator<?> iterator, Object[] expected) {
      assert iterator.hasNext();
      Object[] array = (Object[]) iterator.next();
      assert Arrays.equals(array, expected);
      assert !iterator.hasNext();
   }

   @Indexed(index = "FooIndex")
   public static class Foo {
      private String bar;
      private String baz;

      public Foo(String bar, String baz) {
         this.bar = bar;
         this.baz = baz;
      }

      @Field(name = "bar", store = Store.YES)
      public String getBar() {
         return bar;
      }

      @Field(name = "baz", store = Store.YES)
      public String getBaz() {
         return baz;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o)
            return true;
         if (o == null || getClass() != o.getClass())
            return false;

         Foo foo = (Foo) o;
         if (bar != null ? !bar.equals(foo.bar) : foo.bar != null)
            return false;
         return baz != null ? baz.equals(foo.baz) : foo.baz == null;
      }

      @Override
      public int hashCode() {
         return bar.hashCode();
      }
   }
}
