package org.infinispan.query.blackbox;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.apache.lucene.search.Query;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.VeryLongIndexNamedClass;
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
@Test(groups="functional", testName = "query.blackbox.ClusteredCacheWithLongIndexNameTest")
@CleanupAfterMethod
public class ClusteredCacheWithLongIndexNameTest extends MultipleCacheManagersTest {
   private Cache cache1, cache2, cache3;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultConfiguration();

      List<Cache<String, Person>> caches = createClusteredCaches(3, cacheCfg);
      cache1 = caches.get(0);
      cache2 = caches.get(1);
      cache3 = caches.get(2);
   }

   private ConfigurationBuilder getDefaultConfiguration() {
      ConfigurationBuilder cacheCfg = TestCacheManagerFactory.getDefaultCacheConfiguration(transactionsEnabled(), false);
      cacheCfg.
            clustering()
            .cacheMode(getCacheMode()).sync()
            .indexing()
            .index(Index.ALL)
            .addIndexedEntity(VeryLongIndexNamedClass.class)
            .addProperty("default.directory_provider", "ram")
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
      prepareData();

      SearchManager sm = Search.getSearchManager(cache3);
      QueryBuilder qb = sm.buildQueryBuilderForClass(VeryLongIndexNamedClass.class).get();
      Query q = qb.keyword().wildcard().onField("name").matching("value*").createQuery();
      CacheQuery<?> cq = sm.getQuery(q, VeryLongIndexNamedClass.class);

      assertEquals(100, cq.getResultSize());

      addClusterEnabledCacheManager(getDefaultConfiguration());
      TestingUtil.waitForStableTopology(cache(0), cache(1), cache(2), cache(3));

      sm = Search.getSearchManager(cache(3));
      qb = sm.buildQueryBuilderForClass(VeryLongIndexNamedClass.class).get();
      q = qb.keyword().wildcard().onField("name").matching("value*").createQuery();
      cq = sm.getQuery(q, VeryLongIndexNamedClass.class);

      assertEquals(100, cq.getResultSize());
   }

   private void prepareData() {
      VeryLongIndexNamedClass obj = null;

      for(int i = 0; i < 100; i++) {
         obj = new VeryLongIndexNamedClass("value" + i);
         cache1.put("key" + i, obj);
      }
   }
}
