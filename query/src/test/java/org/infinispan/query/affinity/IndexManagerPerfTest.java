package org.infinispan.query.affinity;

import static java.util.stream.IntStream.range;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESMETADATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_LOCKING_CACHENAME;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Performance test for AffinityIndexManager/InfinispanIndexManager with different configurations. Accepts the following parameters and values:
 * <p>
 * <ul>
 * <li>indexmanager: The FQCN of the IndexManager to test. Default is {@link BaseAffinityTest#DEFAULT_INDEX_MANAGER}</li>
 * <li>index_nodes: The number of nodes in the cluster that will write data with cache.put. Default is {@link BaseAffinityTest#DEFAULT_INDEXING_NODES}</li>
 * <li>query_nodes: The number of nodes in the cluster that will execute queries, Default is {@link BaseAffinityTest#DEFAULT_QUERYING_NODES}</li>
 * <li>index_threads_per_node: The number of threads for each writer node. Default {@link BaseAffinityTest#DEFAULT_INDEXING_THREADS_PER_NODE}</li>
 * <li>query_threads_per_node: The number of threads for each querying node. Default {@link BaseAffinityTest#DEFAULT_QUERYING_THREADS_PER_NODE}</li>
 * <li>entries: The number of entries to insert in the cache. Default {@link BaseAffinityTest#DEFAULT_NUM_ENTRIES}</li>
 * <li>query_type: either TERM or MATCH_ALL, Default {@link BaseAffinityTest#DEFAULT_QUERY_TYPE}</li>
 * <li>shards: the number of shards (AffinityIndexManager only). Default is {@link BaseAffinityTest#DEFAULT_NUM_SEGMENTS}, meaning 1 Infinispan segments equals to 1 Hibernate Search shard.</li>
 * <li>segments: the number of Infinispan segments, default to 256</li>
 * <li>worker: indexing backend worker: sync or async, default {@link BaseAffinityTest#DEFAULT_WORKER}</li>
 * <li>reader_strategy: shared, not-shared or async. Default is shared</li>
 * <li>reader_refresh: if reader_strategy is async, will configure the reader refresh frequency in milliseconds.</li>
 * <li>test: the scenario to test. Either IndexManagerPerfTest#testQueryOnly or IndexManagerPerfTest#testQueryWithWrites()</li>
 * <li>backend worker: sync or async</li>
 * <li>log4j.configurationFile</li>
 * </ul>
 * <p>
 * Command line examples:
 * <p>
 * Test the {@link AffinityIndexManager} with 3 nodes doing writes with 3 thread each, another node doing Term
 * queries with 1 thread, 15K entries, 16 shards, sync worker
 * <pre>
 * mvn -Ptest-stress -Dentries=15000 -Dquery_type=TERM -Dshards=16 -Dtest=IndexManagerPerfTest#testQueryWithWrites verify
 * </pre>
 * Test the {@link AffinityIndexManager} with 2 nodes doing queries with 2 threads each, 1M entries, async worker, custom logfile:
 * <pre>
 *
 * mvn -Ptest-stress -Dquery_nodes=2 -Dquery_threads_per_node=2 -Dentries=1000000 -Dquery_type=TERM -Dworker=async \
 * -Dlog4j.configurationFile=log4j2.xml
 * -Dtest=IndexManagerPerfTest#testQueryOnly verify
 * </pre>
 *
 * @since 9.0
 */
@Test(groups = "stress", testName = "query.IndexManagerPerfTest", timeOut = 15*60*1000)
public class IndexManagerPerfTest extends BaseAffinityTest {

   private final AtomicInteger globalCounter = new AtomicInteger(0);
   private List<Node> nodes = new ArrayList<>();

   @Test
   public void testQueryWithWrites() throws Exception {
      nodes.addAll(range(0, getIndexingNodes()).boxed()
            .map(i -> new IndexingNode(getIndexThreadsPerNode(), globalCounter)).collect(Collectors.toList()));
      nodes.addAll(range(0, getQueryingNodes()).boxed()
            .map(i -> new QueryingNode(getQueryThreadsPerNode(), globalCounter, getQueryType())).collect(Collectors.toList()));
      nodes.forEach(Node::addToCluster);
      waitForClusterToForm();
      warmup();
      nodes.get(0).cacheManager.getCache().clear();

      summarizeReadWriteTest(runTests());
   }

   @Test
   public void testQueryOnly() throws Exception {
      nodes.addAll(range(0, getQueryingNodes()).boxed()
            .map(i -> new TimeBoundQueryNode(1L, TimeUnit.MINUTES, 100L, TimeUnit.MILLISECONDS,
                  getQueryThreadsPerNode(), getQueryType()))
            .collect(Collectors.toList()));
      nodes.forEach(Node::addToCluster);
      waitForClusterToForm();
      addDataToCluster(getNumEntries(), ProcessorInfo.availableProcessors() * 2);
      warmup();

      summarizeQueryOnlyTest(runTests());
   }

   private void summarizeReadWriteTest(long totalTimeMs) {
      Stream<NodeSummary> queryNodesSummary = getNodeSummary(QueryingNode.class, totalTimeMs);
      Stream<NodeSummary> nodeSummary = getNodeSummary(IndexingNode.class, totalTimeMs);

      nodeSummary.forEach(NodeSummary::outputHistogram);

      Optional<double[]> queryStats = averageQueryStats(queryNodesSummary);
      String query90th = queryStats.map(u -> String.valueOf(u[0])).orElse("N/A");
      String qps = queryStats.map(u -> String.valueOf(u[1])).orElse("N/A");

      double totalTimeSeconds = totalTimeMs / 1000.0d;

      Double writeOpsPerSecond = getNumEntries() / (totalTimeMs / 1000.0d);

      System.out.printf("[Done in %fs] Shards: %s, index thread per node: %d, Query 90th: %s, QPS: %s, Put/s: %f\n",
            totalTimeSeconds, getNumShards(), getIndexThreadsPerNode(), query90th, qps, writeOpsPerSecond);
   }

   private void summarizeQueryOnlyTest(long totalTimeMs) {
      Stream<NodeSummary> queryNodesSummary = getNodeSummary(QueryingNode.class, totalTimeMs);

      double[] queryStats = averageQueryStats(queryNodesSummary).get();
      double totalTimeSeconds = totalTimeMs / 1000.0d;

      System.out.printf("[Done in %fs] Shards: %s, Query threads per node (%d nodes querying): %d, " +
                  "Query 90th: %f, QPS: %f\n",
            totalTimeSeconds, getNumShards(), getQueryThreadsPerNode(), getQueryingNodes(), queryStats[0], queryStats[1]);
   }

   private Stream<NodeSummary> getNodeSummary(Class<? extends Node> type, long totalTimeMs) {
      return nodes.stream().filter(n -> type.isAssignableFrom(n.getClass())).map(node -> node.getNodeSummary(totalTimeMs));
   }

   private Optional<double[]> averageQueryStats(Stream<NodeSummary> queryNodesSummary) {
      return queryNodesSummary.map(n -> new double[]{n.getValueAtPercentile(90), n.getOpsPerSecond()})
            .reduce((a, b) -> new double[]{(a[0] + b[0]) / 2, b[0] + b[1]});
   }

   @AfterMethod
   public void after() {
      nodes.forEach(Node::kill);
   }

   private long runTests() {
      long start = System.currentTimeMillis();
      nodes.stream().map(Node::run).parallel().forEach(CompletableFuture::join);
      assertDocsIndexed(30000L);
      long stop = System.currentTimeMillis();

      return stop - start;
   }

   private void addDataToCluster(int entries, int threads) {
      nodes.forEach(node -> {
         Cache<String, Entity> cache = node.cache;
         range(0, threads).boxed().parallel().forEach(t -> {
            int id;
            do {
               id = globalCounter.incrementAndGet();
               if (id <= entries) {
                  cache.put(String.valueOf(id), new Entity(id));
                  System.out.println("put " + id);
               }
            }
            while (id <= entries);
         });
      });
   }

   private void warmup() {
      System.out.println("Warmup started");
      nodes.stream().parallel().forEach(Node::warmup);
      System.out.println("Warmup finished");
   }

   @Override
   protected EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilder builder, TransportFlags flags) {
      GlobalConfigurationBuilder gc = GlobalConfigurationBuilder.defaultClusteredBuilder();
      EmbeddedCacheManager cm = TestCacheManagerFactory.newDefaultCacheManager(true,
            gc, builder, false);
      cm.defineConfiguration(DEFAULT_LOCKING_CACHENAME, getLockCacheConfig());
      cm.defineConfiguration(DEFAULT_INDEXESMETADATA_CACHENAME, getMetadataCacheConfig());
      cm.defineConfiguration(DEFAULT_INDEXESDATA_CACHENAME, getDataCacheConfig());
      cacheManagers.add(cm);
      return cm;
   }

   @AfterMethod
   @Override
   protected void clearContent() {
   }

   @Override
   protected void createCacheManagers() throws Throwable {
   }


   @Override
   protected String getReaderStrategy() {
      return "async";
   }
}
