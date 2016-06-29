package org.infinispan.query.affinity;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.IntStream.range;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Tests behaviour of the AffinityIndexManager under topology changes.
 *
 * @since 9.0
 */
@Test(groups = "stress", testName = "query.AffinityTopologyChangeTest")
public class AffinityTopologyChangeTest extends BaseAffinityTest {

   private static final int THREADS_PER_NODE = 2;
   private int ENTRIES = 500;

   private AtomicInteger globalCounter = new AtomicInteger(0);
   private Node indexing1, indexing2, indexing3, querying;

   @BeforeMethod
   public void prepare() {
      indexing1 = new IndexingNode();
      indexing2 = new IndexingNode();
      indexing3 = new IndexingNode();
      querying = new QueryingNode();
   }

   @AfterMethod
   public void after() {
      indexing3.kill();
      querying.kill();
      indexing2.kill();
      indexing1.kill();
   }


   @Test
   public void testReadWriteUnderTopologyChanges() throws Exception {
      CompletableFuture<?> f1 = indexing1.addToCluster().run();
      CompletableFuture<?> f2 = indexing2.addToCluster().run();

      eventually(() -> indexing2.cacheManager.getMembers().size() == 2);

      CompletableFuture<?> f3 = indexing3.addToCluster().run();
      CompletableFuture<?> f4 = querying.addToCluster().run();

      CompletableFuture.allOf(f1, f2, f3, f4).join();

      eventually(() -> {
         int resultSize = Search.getSearchManager(pickCache()).getQuery(new MatchAllDocsQuery()).getResultSize();
         System.out.println(resultSize);
         return resultSize == ENTRIES;
      });
   }

   abstract class Node {
      protected EmbeddedCacheManager cacheManager;
      protected Cache<String, Entity> cache;

      Node addToCluster() {
         cacheManager = addClusterEnabledCacheManager(cacheCfg);
         cache = cacheManager.getCache();
         return this;
      }

      void kill() {
         killCacheManagers(cacheManager);
         cacheManagers.remove(cacheManager);
      }

      abstract CompletableFuture<Void> run();

   }

   abstract class TaskNode extends Node {
      private final ExecutorService executorService;

      TaskNode() {
         this(THREADS_PER_NODE);
      }

      TaskNode(int nThreads) {
         executorService = Executors.newFixedThreadPool(nThreads);
      }

      void kill() {
         executorService.shutdownNow();
         super.kill();
      }

      abstract void executeTask();

      CompletableFuture<Void> run() {
         List<CompletableFuture<?>> futures = range(0, THREADS_PER_NODE).boxed().map(t -> supplyAsync(() -> {
            executeTask();
            return null;
         }, executorService)).collect(Collectors.toList());
         return CompletableFuture.allOf(futures.toArray(new CompletableFuture[THREADS_PER_NODE]));
      }
   }

   private class IndexingNode extends TaskNode {

      @Override
      void executeTask() {
         int id = 0;
         while (id <= ENTRIES) {
            id = globalCounter.incrementAndGet();
            if (id <= ENTRIES) {
               cache.put(String.valueOf(id), new Entity(id));
               System.out.printf("Put %d\n", id);
            }
         }
      }
   }

   private class QueryingNode extends TaskNode {

      static final int QUERY_INTERVAL_MS = 500;

      QueryingNode() {
         super(1);
      }

      @Override
      void executeTask() {
         int id = globalCounter.get();
         CacheQuery q = Search.getSearchManager(cache).getQuery(new MatchAllDocsQuery(), Entity.class);
         while (id <= ENTRIES) {
            int size = q.list().size();
            assertTrue(size > 0 && size <= ENTRIES);
            id = globalCounter.get();
            try {
               Thread.sleep(QUERY_INTERVAL_MS);
            } catch (InterruptedException ignored) {
            }
         }
      }

   }

   @Override
   protected EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilder builder, TransportFlags flags) {
      GlobalConfigurationBuilder gc = GlobalConfigurationBuilder.defaultClusteredBuilder();
      TestCacheManagerFactory.amendTransport(gc);
      EmbeddedCacheManager cm = TestCacheManagerFactory.newDefaultCacheManager(true,
              gc, builder, false);
      cacheManagers.add(cm);
      return cm;
   }

}
