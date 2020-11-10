package org.infinispan.query.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.search.mapper.common.impl.EntityReferenceImpl;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.projection.ProjectionTest")
public class ProjectionTest extends SingleCacheManagerTest {

   private QueryFactory queryFactory;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Foo.class);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(cfg);
      Cache<Object, Object> cache = cacheManager.getCache();
      queryFactory = Search.getQueryFactory(cache);
      return cacheManager;
   }

   @Test
   public void testQueryProjectionWithSingleField() {
      cache.put("1", new Foo("bar1", "baz1"));
      Query<?> cacheQuery = createProjectionQuery("bar");
      assertQueryReturns(cacheQuery, new Object[]{"bar1"});
   }

   @Test
   public void testQueryProjectionWithMultipleFields() {
      cache.put("1", new Foo("bar1", "baz1"));
      Query<?> cacheQuery = createProjectionQuery("bar", "baz");
      assertQueryReturns(cacheQuery, new Object[]{"bar1", "baz1"});
   }

   @Test
   public void testMixedProjections() {
      Foo foo = new Foo("bar1", "baz4");
      cache.put("1", foo);
      Query<?> cacheQuery = createProjectionQuery(
            "baz",
            "bar"
      );
      assertQueryReturns(cacheQuery, new Object[]{foo.baz, foo.bar});
   }

   private <T> Query<T> createProjectionQuery(String... projection) {
      String selectClause = String.join(",", projection);
      String q = String.format("SELECT %s FROM %s WHERE bar:'bar1'", selectClause, Foo.class.getName());
      return queryFactory.create(q);
   }

   private void assertQueryReturns(Query<?> cacheQuery, Object expected) {
      assertQueryListContains(cacheQuery.execute().list(), expected);
      try (CloseableIterator<?> eagerIterator = cacheQuery.iterator()) {
         assertQueryIteratorContains(eagerIterator, expected);
      }
   }

   private void assertQueryListContains(List<?> list, Object expected) {
      assert list.size() == 1;
      Object value = list.get(0);
      assertThat(value).isEqualTo(expected);
   }

   private void assertQueryIteratorContains(CloseableIterator<?> iterator, Object expected) {
      assert iterator.hasNext();
      Object value = iterator.next();
      assertThat(value).isEqualTo(expected);
      assert !iterator.hasNext();
   }

   private static EntityReferenceImpl entityReference(Class<?> type, String key) {
      return new EntityReferenceImpl(PojoRawTypeIdentifier.of(type), type.getSimpleName(), key);
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
