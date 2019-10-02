package org.infinispan.query.distributed;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.query.test.Transaction;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Test for the non blocking MassIndexer
 *
 * @author gustavonalle
 * @since 7.1
 */
@Test(groups = "functional", testName = "query.distributed.AsyncMassIndexTest")
public class AsyncMassIndexTest extends MultipleCacheManagersTest {

   protected static final int NUM_NODES = 2;

   protected CleanupPhase cleanup = CleanupPhase.AFTER_METHOD;

   @Override
   @SuppressWarnings("unchecked")
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheCfg
            .indexing()
            .index(Index.PRIMARY_OWNER)
            .addIndexedEntity(Transaction.class)
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      createClusteredCaches(NUM_NODES, QueryTestSCI.INSTANCE, cacheCfg);
      waitForClusterToForm(getDefaultCacheName());
   }



   private void populate(int elements) throws Exception {
      Cache<Integer, Transaction> cache = cache(0);
      for (int i = 0; i < elements; i++) {
         cache.put(i, new Transaction(i + 200, "bytes"));
      }
   }

   @Test
   public void testListener() throws Exception {
      Cache<Integer, Transaction> cache = cache(0);
      int elements = 50;
      populate(elements);

      SearchManager searchManager = Search.getSearchManager(cache);

      CompletableFuture<Void> future = searchManager.getMassIndexer().startAsync();

      final CountDownLatch endLatch = new CountDownLatch(1);
      future.whenComplete((v, t) -> {
         endLatch.countDown();
      });
      endLatch.await();

      checkIndex(elements, Transaction.class);
   }

   protected void checkIndex(int expectedNumber, Class<?> entity) {
      Cache<Integer, Transaction> c = cache(0);
      SearchManager searchManager = Search.getSearchManager(c);
      CacheQuery<?> q = searchManager.getQuery(new MatchAllDocsQuery(), entity);
      int resultSize = q.getResultSize();
      assertEquals(expectedNumber, resultSize);
   }

}
