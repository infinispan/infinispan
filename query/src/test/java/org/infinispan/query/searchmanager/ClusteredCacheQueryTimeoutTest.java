package org.infinispan.query.searchmanager;

import java.util.concurrent.TimeUnit;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * The test covers the timeout functionality for ClusteredCacheQuery class. At the moment it is not implemented, so throws
 * UnsupportedException.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.searchmanager.ClusteredCacheQueryTimeoutTest")
public class ClusteredCacheQueryTimeoutTest extends MultipleCacheManagersTest {
   private Cache cache1;

   @Override
   protected void createCacheManagers() throws Throwable {
      // A query will not be broadcasted for REPL caches, so make this DIST
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheCfg.indexing()
            .enable()
            .addIndexedEntity(Foo.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      createClusteredCaches(2, cacheCfg);
      cache1 = cache(0);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = "Clustered queries do not support timeouts yet.")
   public void testClusteredQueryCacheTimeout() {
      SearchManager searchManager = Search.getSearchManager(cache1);

      String q = String.format("FROM %s WHERE bar:'fakebar'", Foo.class.getName());
      CacheQuery<?> query = searchManager.getQuery(q);
      query.timeout(1, TimeUnit.NANOSECONDS);
   }

   @Indexed(index = "FooIndex")
   public class Foo {
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
