package org.infinispan.query.distributed;

import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.Search;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.test.Block;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.query.test.Transaction;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Test for the MassIndexer for different entities sharing the same cache.
 *
 * @author gustavonalle
 * @since 7.1
 */
@Test(groups = "functional", testName = "query.distributed.OverlappingIndexMassIndexTest")
public class OverlappingIndexMassIndexTest extends MultipleCacheManagersTest {

   protected static final int NUM_NODES = 3;
   protected List<Cache<String, Object>> caches = new ArrayList<>(NUM_NODES);

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cacheCfg
            .indexing()
            .enable()
            .addIndexedEntity(Transaction.class)
            .addIndexedEntity(Block.class)
            .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP)
            .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName());

      createClusteredCaches(NUM_NODES, QueryTestSCI.INSTANCE, cacheCfg);

      waitForClusterToForm(getDefaultCacheName());

      caches = caches();
   }

   public void testReindex() {
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
         int query1ResultSize = TestQueryHelperFactory.queryAll(c, entity).size();
         int query2ResultSize = TestQueryHelperFactory.queryAll(c, otherEntity).size();
         assertEquals(expectedNumber, query1ResultSize);
         assertEquals(otherExpected, query2ResultSize);
      }
   }

   protected void runMassIndexer() {
      Cache<?, ?> cache = caches.get(0);
      join((Search.getIndexer(cache)).run());
   }
}
