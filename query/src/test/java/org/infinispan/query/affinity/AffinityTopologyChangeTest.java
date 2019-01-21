package org.infinispan.query.affinity;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests behaviour of the AffinityIndexManager under topology changes.
 *
 * @since 9.0
 */
@Test(groups = "stress", testName = "query.AffinityTopologyChangeTest", timeOut = 15*60*1000)
public class AffinityTopologyChangeTest extends BaseAffinityTest {
   private final AtomicInteger globalCounter = new AtomicInteger(0);
   private Node indexing1, indexing2, indexing3, querying;

   @BeforeMethod
   public void prepare() {
      indexing1 = new IndexingNode(getIndexThreadsPerNode(), globalCounter);
      indexing2 = new IndexingNode(getIndexThreadsPerNode(), globalCounter);
      indexing3 = new IndexingNode(getIndexThreadsPerNode(), globalCounter);
      querying = new QueryingNode(getQueryThreadsPerNode(), globalCounter, QueryType.MATCH_ALL);
   }

   @AfterMethod(alwaysRun = true)
   public void after() {
      indexing3.kill();
      querying.kill();
      indexing2.kill();
      indexing1.kill();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
   }

   @Test
   public void testReadWriteUnderTopologyChanges() throws Exception {
      CompletableFuture<?> f1 = indexing1.addToCluster().run();
      CompletableFuture<?> f2 = indexing2.addToCluster().run();

      eventuallyEquals(2, () -> indexing2.cacheManager.getMembers().size());

      CompletableFuture<?> f3 = indexing3.addToCluster().run();
      CompletableFuture<?> f4 = querying.addToCluster().run();

      CompletableFuture.allOf(f1, f2, f3, f4).join();

      assertDocsIndexed(50000L);

   }

}
