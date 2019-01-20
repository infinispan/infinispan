package org.infinispan.query.distributed;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.Cache;
import org.infinispan.commons.test.ThreadLeakChecker;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jboss.byteman.agent.TransformListener;
import org.jboss.byteman.contrib.bmunit.BMNGListener;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;


/**
 * Tests for running MassIndexer on async indexing backend
 *
 * @author gustavonalle
 * @since 7.2
 */
@Test(groups = "functional", testName = "query.distributed.MassIndexerAsyncBackendTest")
@Listeners(BMNGListener.class)
public class MassIndexerAsyncBackendTest extends MultipleCacheManagersTest {

   protected static final int NUM_NODES = 2;
   protected static final int NUM_ENTRIES = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < NUM_NODES; i++) {
         EmbeddedCacheManager cacheManager = TestCacheManagerFactory.fromXml("dist-indexing-async.xml");
         registerCacheManager(cacheManager);
      }
      waitForClusterToForm("default", "LuceneIndexesMetadata", "LuceneIndexesData", "LuceneIndexesLocking");
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      // DistributedExecutorMassIndexer leaks executor, see ISPN-7606
      ThreadLeakChecker.ignoreThreadsContaining("DefaultExecutorService-");
      // ByteMan thread stopped after our thread leak check
      ThreadLeakChecker.ignoreThreadsMatching(thread -> thread instanceof TransformListener);

      super.destroy();
   }

   @Test
   @BMRule(name = "Delay the purge of the index",
           targetClass = "org.hibernate.search.backend.impl.lucene.works.PurgeAllWorkExecutor",
           targetMethod = "performWork",
           action = "delay(500)"
   )
   public void testMassIndexOnAsync() throws Exception {
      final Cache<Object, Object> cache = caches().get(0);

      for (int i = 0; i < NUM_ENTRIES; i++) {
         cache.getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(String.valueOf(i), new Transaction(i, "12345"));
      }

      for (Cache c : caches()) {
         Search.getSearchManager(c).getMassIndexer().start();
         assertAllIndexed(c);
      }

   }

   private void assertAllIndexed(final Cache cache) {
      eventually(() -> {
         int size = Search.getSearchManager(cache).getQuery(new MatchAllDocsQuery(), Transaction.class).list().size();
         return size == NUM_ENTRIES;
      });
   }

}
