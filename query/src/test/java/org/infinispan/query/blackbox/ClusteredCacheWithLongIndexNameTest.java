package org.infinispan.query.blackbox;

import static org.testng.AssertJUnit.assertEquals;

import java.io.Serializable;
import java.util.List;

import org.apache.lucene.search.Query;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * The test verifies the issue ISPN-3092.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheWithLongIndexNameTest")
@CleanupAfterMethod
public class ClusteredCacheWithLongIndexNameTest extends MultipleCacheManagersTest {

   private Cache<String, ClassWithLongIndexName> cache0, cache1, cache2;

   @Override
   protected void createCacheManagers() throws Throwable {
      List<Cache<String, ClassWithLongIndexName>> caches = createClusteredCaches(3, getDefaultConfiguration());
      cache0 = caches.get(0);
      cache1 = caches.get(1);
      cache2 = caches.get(2);
   }

   private ConfigurationBuilder getDefaultConfiguration() {
      ConfigurationBuilder cacheCfg = TestCacheManagerFactory.getDefaultCacheConfiguration(transactionsEnabled(), false);
      cacheCfg.
            clustering()
            .cacheMode(getCacheMode())
            .indexing()
            .index(Index.ALL)
            .addIndexedEntity(ClassWithLongIndexName.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return cacheCfg;
   }

   public boolean transactionsEnabled() {
      return false;
   }

   public CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   public void testAdditionOfNewNode() {
      for (int i = 0; i < 100; i++) {
         cache0.put("key" + i, new ClassWithLongIndexName("value" + i));
      }

      SearchManager sm2 = Search.getSearchManager(cache2);
      Query q = sm2.buildQueryBuilderForClass(ClassWithLongIndexName.class).get()
            .keyword().wildcard().onField("name").matching("value*").createQuery();
      CacheQuery<?> cq = sm2.getQuery(q, ClassWithLongIndexName.class);
      assertEquals(100, cq.getResultSize());

      addClusterEnabledCacheManager(getDefaultConfiguration());
      TestingUtil.waitForNoRebalance(cache(0), cache(1), cache(2), cache(3));

      SearchManager sm3 = Search.getSearchManager(cache(3));
      q = sm3.buildQueryBuilderForClass(ClassWithLongIndexName.class).get()
            .keyword().wildcard().onField("name").matching("value*").createQuery();
      cq = sm3.getQuery(q, ClassWithLongIndexName.class);
      assertEquals(100, cq.getResultSize());
   }

   // index name as in bug description
   @Indexed(index = "default_taskworker-java__com.google.appengine.api.datastore.Entity")
   private static class ClassWithLongIndexName implements Serializable, ExternalPojo {

      private static final long serialVersionUID = 1;

      @Field(store = Store.YES)
      String name;

      ClassWithLongIndexName(String name) {
         this.name = name;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         ClassWithLongIndexName that = (ClassWithLongIndexName) o;
         return name != null ? name.equals(that.name) : that.name == null;
      }

      @Override
      public int hashCode() {
         return name != null ? name.hashCode() : 0;
      }

      @Override
      public String toString() {
         return "ClassWithLongIndexName{name='" + name + "'}";
      }
   }
}
