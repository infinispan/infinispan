package org.infinispan.query.searchmanager;

import static org.testng.Assert.fail;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

/**
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 */
@Test(groups = "functional", testName = "query.searchmanager.TimeoutTest")
public class TimeoutTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .indexing()
            .enable()
            .addIndexedEntity(Foo.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Test(enabled = false, description = "ISPN-9469")
   public void timeoutExceptionIsThrown() {
      String q = String.format("FROM %s WHERE bar:'1'", Foo.class.getName());
      Query<?> cacheQuery = Search.getQueryFactory(cache).create(q);
//      cacheQuery.timeout(1, TimeUnit.NANOSECONDS);

      try {
         cacheQuery.list();
         fail("Expected TimeoutException");
      } catch (TimeoutException ignored) {
      }
   }

   @Indexed(index = "FooIndex")
   public static class Foo {
      private String bar;

      public Foo(String bar) {
         this.bar = bar;
      }

      @Field(name = "bar")
      public String getBar() {
         return bar;
      }
   }
}
