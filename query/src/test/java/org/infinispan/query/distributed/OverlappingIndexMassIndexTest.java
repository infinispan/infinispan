package org.infinispan.query.distributed;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Test for the MassIndexer for different entities sharing the same index and cache.
 *
 * @author gustavonalle
 * @since 7.1
 */
@Test(groups = "functional", testName = "query.distributed.OverlappingIndexMassIndexTest")
public class OverlappingIndexMassIndexTest extends MultipleCacheManagersTest {

   protected static final int NUM_NODES = 3;
   protected List<Cache<String, Object>> caches = new ArrayList<>(NUM_NODES);

   @Override
   @SuppressWarnings("unchecked")
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cacheCfg
            .indexing()
            .index(Index.ALL)
            .addIndexedEntity(Transaction.class)
            .addIndexedEntity(Block.class)
            .addProperty("default.directory_provider", "ram")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      List<Cache<String, Object>> cacheList = createClusteredCaches(NUM_NODES, cacheCfg);

      waitForClusterToForm(BasicCacheContainer.DEFAULT_CACHE_NAME);

      for (Cache cache : cacheList) {
         caches.add(cache);
      }
   }

   public void testReindex() throws Exception {
      Transaction t1 = new Transaction(302, "04a27");
      Transaction t2 = new Transaction(256, "ae461");
      Transaction t3 = new Transaction(257, "ac537");
      Block b1 = new Block(1, t1);
      Block b2 = new Block(2, t2);
      Block b3 = new Block(3, t3);

      caches.get(0).put("T1", t1);
      caches.get(0).put("T2", t2);
      caches.get(0).put("T3", t3);
      caches.get(0).put("B1", b1);
      caches.get(0).put("B2", b2);
      caches.get(0).put("B3", b3);

      checkIndex(3, Transaction.class, 3, Block.class);

      runMassIndexer();

      checkIndex(3, Transaction.class, 3, Block.class);

      caches.get(0).clear();

      runMassIndexer();

      checkIndex(0, Transaction.class, 0, Block.class);
   }

   protected void checkIndex(int expectedNumber, Class<?> entity, int otherExpected, Class<?> otherEntity) {
      for (Cache<?, ?> c : caches) {
         SearchManager searchManager = Search.getSearchManager(c);
         CacheQuery query1 = searchManager.getQuery(new MatchAllDocsQuery(), entity);
         CacheQuery query2 = searchManager.getQuery(new MatchAllDocsQuery(), otherEntity);
         assertEquals(expectedNumber, query1.getResultSize());
         assertEquals(otherExpected, query2.getResultSize());
      }
   }

   protected void runMassIndexer() throws Exception {
      Cache cache = caches.get(0);
      SearchManager searchManager = Search.getSearchManager(cache);
      searchManager.getMassIndexer().start();
   }
}
