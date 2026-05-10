package org.infinispan.query.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Objects;

import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.infinispan.Cache;
import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.mapper.common.impl.EntityReferenceImpl;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.projection.ProjectionTest")
public class ProjectionTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Foo.class);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(cfg);
      Cache<Object, Object> cache = cacheManager.getCache();
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
      return cache.query(q);
   }

   private void assertQueryReturns(Query<?> cacheQuery, Object expected) {
      assertQueryListContains(cacheQuery.execute().list(), expected);
      try (CloseableIterator<?> eagerIterator = cacheQuery.iterator()) {
         assertQueryIteratorContains(eagerIterator, expected);
      }
   }

   private void assertQueryListContains(List<?> list, Object expected) {
      assertEquals(1, list.size());
      Object value = list.get(0);
      assertThat(value).isEqualTo(expected);
   }

   private void assertQueryIteratorContains(CloseableIterator<?> iterator, Object expected) {
      assertTrue(iterator.hasNext());
      Object value = iterator.next();
      assertThat(value).isEqualTo(expected);
      assertFalse(iterator.hasNext());
   }

   private static EntityReferenceImpl entityReference(Class<?> type, String key) {
      return new EntityReferenceImpl(PojoRawTypeIdentifier.of(type), type.getSimpleName(), key);
   }

   @Indexed(index = "FooIndex")
   public static class Foo {
      private final String bar;
      private final String baz;

      public Foo(String bar, String baz) {
         this.bar = bar;
         this.baz = baz;
      }

      @Text
      public String getBar() {
         return bar;
      }

      @Basic(projectable = true)
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
         if (!Objects.equals(bar, foo.bar))
            return false;
         return Objects.equals(baz, foo.baz);
      }

      @Override
      public int hashCode() {
         return bar.hashCode();
      }
   }
}
