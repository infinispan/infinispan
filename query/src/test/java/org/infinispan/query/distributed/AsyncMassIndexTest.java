package org.infinispan.query.distributed;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.Search;
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
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheCfg
            .indexing()
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Transaction.class);

      createClusteredCaches(NUM_NODES, QueryTestSCI.INSTANCE, cacheCfg);
      waitForClusterToForm(getDefaultCacheName());
   }

   private void populate(int elements) {
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

      CompletableFuture<Void> future = Search.getIndexer(cache).run().toCompletableFuture();

      final CountDownLatch endLatch = new CountDownLatch(1);
      future.whenComplete((v, t) -> endLatch.countDown());
      endLatch.await();

      checkIndex(elements);
   }

   protected void checkIndex(int expectedNumber) {
      Cache<Integer, Transaction> c = cache(0);
      Query<Transaction> q = c.query("FROM " + Transaction.class.getName());
      long resultSize = q.execute().count().value();
      assertEquals(expectedNumber, resultSize);
   }
}
