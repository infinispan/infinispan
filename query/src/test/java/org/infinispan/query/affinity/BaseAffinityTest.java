package org.infinispan.query.affinity;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.IntStream.range;
import static java.util.stream.IntStream.rangeClosed;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESMETADATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_LOCKING_CACHENAME;
import static org.infinispan.test.TestingUtil.killCacheManagers;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationChildBuilder;
import org.infinispan.configuration.cache.HashConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.IndexingConfigurationBuilder;
import org.infinispan.distribution.ch.impl.AffinityPartitioner;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.AfterMethod;

public abstract class BaseAffinityTest extends MultipleCacheManagersTest {

   private static final String DEFAULT_INDEX_MANAGER = AffinityIndexManager.class.getName();
   private static final int DEFAULT_NUM_ENTRIES = 50;
   private static final int DEFAULT_NUM_SEGMENTS = 256;
   private static final int DEFAULT_NUM_OWNERS = 2;
   private static final int DEFAULT_NUM_SHARDS = AffinityShardIdentifierProvider.DEFAULT_NUMBER_SHARDS;
   private static final int DEFAULT_REMOTE_TIMEOUT_MINUTES = 1;
   private static final int DEFAULT_INDEXING_THREADS_PER_NODE = 3;
   private static final int DEFAULT_QUERYING_THREADS_PER_NODE = 10;
   private static final int DEFAULT_INDEXING_NODES = 3;
   private static final int DEFAULT_QUERYING_NODES = 1;
   private static final String DEFAULT_READER_REFRESH_MS = "100";
   private static final String DEFAULT_WORKER = "sync";
   private static final String DEFAULT_READER = "shared";
   private static final QueryType DEFAULT_QUERY_TYPE = QueryType.TERM;

   private static final String ENTRIES_SYS_PROP = "entries";
   private static final String INDEX_MANAGER_SYS_PROP = "indexmanager";
   private static final String SEGMENTS_SYS_PROP = "segments";
   private static final String SHARDS_SYS_PROP = "shards";
   private static final String INDEX_THREADS_SYS_PROP = "index_threads_per_node";
   private static final String QUERY_THREADS_SYS_PROP = "query_threads_per_node";
   private static final String WORKER_SYS_PROP = "worker";
   private static final String QUERY_TYPE_SYS_PROP = "query_type";
   private static final String INDEXING_NODES_SYS_PROP = "index_nodes";
   private static final String QUERYING_NODES_SYS_PROP = "query_nodes";
   private static final String READER_SYS_PROP = "reader_strategy";
   private static final String READER_REFRESH = "reader_refresh";

   protected Random random = new Random();

   protected String getIndexManager() {
      return System.getProperty(INDEX_MANAGER_SYS_PROP, DEFAULT_INDEX_MANAGER);
   }

   protected int getNumEntries() {
      return Integer.getInteger(ENTRIES_SYS_PROP, DEFAULT_NUM_ENTRIES);
   }

   protected int getNumSegments() {
      return Integer.getInteger(SEGMENTS_SYS_PROP, DEFAULT_NUM_SEGMENTS);
   }

   protected int getNumShards() {
      return Integer.getInteger(SHARDS_SYS_PROP, DEFAULT_NUM_SHARDS);
   }

   protected int getRemoteTimeoutInMinutes() {
      return DEFAULT_REMOTE_TIMEOUT_MINUTES;
   }

   protected int getIndexThreadsPerNode() {
      return Integer.getInteger(INDEX_THREADS_SYS_PROP, DEFAULT_INDEXING_THREADS_PER_NODE);
   }

   protected int getQueryThreadsPerNode() {
      return Integer.getInteger(QUERY_THREADS_SYS_PROP, DEFAULT_QUERYING_THREADS_PER_NODE);
   }

   protected int getQueryingNodes() {
      return Integer.getInteger(QUERYING_NODES_SYS_PROP, DEFAULT_QUERYING_NODES);
   }

   protected int getIndexingNodes() {
      return Integer.getInteger(INDEXING_NODES_SYS_PROP, DEFAULT_INDEXING_NODES);
   }

   protected String getWorker() {
      return System.getProperty(WORKER_SYS_PROP, DEFAULT_WORKER);
   }

   protected String getReaderStrategy() {
      return System.getProperty(READER_SYS_PROP, DEFAULT_READER);
   }

   protected String getReaderRefresh() {
      return System.getProperty(READER_REFRESH, DEFAULT_READER_REFRESH_MS);
   }

   protected QueryType getQueryType() {
      String sysProp = System.getProperty(QUERY_TYPE_SYS_PROP);
      return sysProp == null ? DEFAULT_QUERY_TYPE : QueryType.valueOf(sysProp.toUpperCase());
   }

   protected Map<String, String> getIndexingProperties() {
      String indexManager = getIndexManager();
      Map<String, String> props = new HashMap<>(5);
      props.put("hibernate.search.lucene_version", "LUCENE_CURRENT");
      props.put("entity.indexmanager", indexManager);
      props.put("default.worker.execution", getWorker());
      props.put("default.reader.strategy", getReaderStrategy());
      props.put("default.indexwriter.merge_factor", "30");
      props.put("default.indexwriter.merge_max_size", "1024");
      props.put("default.indexwriter.ram_buffer_size", "256");
      props.put("default.reader.async_refresh_period_ms", getReaderRefresh());
      if (indexManager.equals(AffinityIndexManager.class.getName())) {
         props.put("entity.sharding_strategy.nbr_of_shards", String.valueOf(getNumShards()));
      }
      return props;
   }

   protected ConfigurationBuilder getBaseConfigBuilder(CacheMode cacheMode) {
      ConfigurationBuilder configBuilder = getDefaultClusteredCacheConfig(cacheMode, false);
      HashConfigurationBuilder hashConfigurationBuilder = configBuilder
            .clustering()
            .remoteTimeout(getRemoteTimeoutInMinutes(), TimeUnit.MINUTES)
            .hash().numSegments(getNumSegments()).numOwners(getNumOwners());
      if (getIndexManager().equals(AffinityIndexManager.class.getName())) {
         hashConfigurationBuilder.keyPartitioner(new AffinityPartitioner());
      }
      return configBuilder;
   }

   protected ConfigurationBuilder getDefaultCacheConfigBuilder() {
      ConfigurationBuilder baseConfigBuilder = getBaseConfigBuilder(CacheMode.DIST_SYNC);
      IndexingConfigurationBuilder indexCfgBuilder = baseConfigBuilder.indexing()
            .index(Index.PRIMARY_OWNER).addIndexedEntity(Entity.class);
      this.getIndexingProperties()
            .entrySet().forEach(entry -> indexCfgBuilder.addProperty(entry.getKey(), entry.getValue()));
      return baseConfigBuilder;
   }

   protected ConfigurationChildBuilder getBaseIndexCacheConfig(CacheMode mode) {
      ConfigurationBuilder baseConfig = getBaseConfigBuilder(mode);
      return baseConfig.indexing().index(Index.NONE);
   }

   protected Configuration getLockCacheConfig() {
      return getBaseIndexCacheConfig(CacheMode.REPL_SYNC).build();
   }

   protected Configuration getMetadataCacheConfig() {
      return getBaseIndexCacheConfig(CacheMode.DIST_SYNC).build();
   }

   protected Configuration getDataCacheConfig() {
      return getBaseIndexCacheConfig(CacheMode.DIST_SYNC).build();
   }

   @Override
   protected EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilder builder, TransportFlags flags) {
      EmbeddedCacheManager cm = super.addClusterEnabledCacheManager(builder, flags);
      configureIndexCaches(Collections.singleton(cm));
      return cm;
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      cache(0).clear();
   }

   private void configureIndexCaches(Collection<EmbeddedCacheManager> managers) {
      managers.forEach(
            cm -> {
               cm.defineConfiguration(DEFAULT_LOCKING_CACHENAME, getLockCacheConfig());
               cm.defineConfiguration(DEFAULT_INDEXESMETADATA_CACHENAME, getMetadataCacheConfig());
               cm.defineConfiguration(DEFAULT_INDEXESDATA_CACHENAME, getDataCacheConfig());
            }
      );
   }

   protected int getNumOwners() {
      return DEFAULT_NUM_OWNERS;
   }

   synchronized Cache<String, Entity> pickCache() {
      List<Cache<String, Entity>> caches = caches();
      return caches.get(random.nextInt(caches.size()));
   }

   protected void assertDocsIndexed(long millis) {
      int numEntries = getNumEntries();
      this.eventually(() -> {
         CacheQuery<Object[]> query = Search.getSearchManager(pickCache()).getQuery(new MatchAllDocsQuery()).projection("val");
         Set<Integer> indexedDocsIds = query.list().stream().map(projections -> (Integer) projections[0]).collect(Collectors.toSet());
         int resultSize = indexedDocsIds.size();
         return resultSize == numEntries;
      }, millis);
   }

   void populate(int initialId, int finalId) {
      rangeClosed(initialId, finalId).forEach(i -> pickCache().put(String.valueOf(i), new Entity(i)));
   }

   synchronized void addNode() {
      addClusterEnabledCacheManager(getDefaultCacheConfigBuilder());
      waitForClusterToForm();
   }

   abstract class Node {
      protected EmbeddedCacheManager cacheManager;
      protected Cache<String, Entity> cache;
      final int WARMUP_ITERATIONS = 1000;
      final LatencyStats latencyStats = new LatencyStats();

      Node addToCluster() {
         cacheManager = addClusterEnabledCacheManager(getDefaultCacheConfigBuilder());
         cache = cacheManager.getCache();
         return this;
      }

      void kill() {
         killCacheManagers(cacheManager);
         cacheManagers.remove(cacheManager);
      }

      abstract CompletableFuture<Void> run();

      abstract void warmup();

      NodeSummary getNodeSummary(long timeMs) {
         Histogram intervalHistogram = latencyStats.getIntervalHistogram();
         return new NodeSummary(intervalHistogram, timeMs);
      }

   }

   abstract class TaskNode extends Node {
      private final ExecutorService executorService;
      private int nThreads;
      protected AtomicInteger globalCounter;

      TaskNode(int nThreads, AtomicInteger globalCounter) {
         executorService = Executors.newFixedThreadPool(nThreads, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
               Thread thread = new Thread(r);
               thread.setName("TaskNode");
               return thread;
            }
         });
         this.nThreads = nThreads;
         this.globalCounter = globalCounter;
      }

      void kill() {
         executorService.shutdownNow();
         super.kill();
      }

      abstract void executeTask();

      abstract void warmup();

      CompletableFuture<Void> run() {
         List<CompletableFuture<?>> futures = range(0, nThreads).boxed().map(t -> supplyAsync(() -> {
            executeTask();
            return null;
         }, executorService)).collect(Collectors.toList());
         return CompletableFuture.allOf(futures.toArray(new CompletableFuture[nThreads]));
      }
   }

   class IndexingNode extends TaskNode {
      IndexingNode(int nThreads, AtomicInteger globalCounter) {
         super(nThreads, globalCounter);
      }

      @Override
      void executeTask() {
         int id = 0;
         int numEntries = getNumEntries();
         while (id <= numEntries) {
            id = globalCounter.incrementAndGet();
            if (id <= numEntries) {
               long start = System.nanoTime();
               cache.put(String.valueOf(id), new Entity(id));
               System.out.println("Put " + id);
               latencyStats.recordLatency(System.nanoTime() - start);
            }
         }
      }

      @Override
      void warmup() {
         for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            cache.put(String.valueOf(random.nextInt(WARMUP_ITERATIONS)), new Entity(i));
            if (i % 100 == 0) {
               System.out.printf("[Warmup] Added %d entries\n", i);
            }
         }
      }
   }

   enum QueryType {MATCH_ALL, TERM}

   class QueryingNode extends TaskNode {

      static final int QUERY_INTERVAL_MS = 10;
      final QueryType queryType;

      QueryingNode(int nThreads, AtomicInteger globalCounter, QueryType queryType) {
         super(nThreads, globalCounter);
         this.queryType = queryType;
      }

      protected Query createLuceneQuery() {
         if (queryType == QueryType.MATCH_ALL) {
            return new MatchAllDocsQuery();
         }
         if (queryType == QueryType.TERM) {
            return Search.getSearchManager(cache)
                  .buildQueryBuilderForClass(Entity.class).get()
                  .keyword().onField("val").matching(getRandomTerm())
                  .createQuery();
         }
         return null;
      }

      protected int getRandomTerm() {
         return Math.round((float) (globalCounter.get() * 0.75));
      }

      @Override
      void executeTask() {
         int id = globalCounter.get();
         Query luceneQuery = createLuceneQuery();
         CacheQuery q = Search.getSearchManager(cache).getQuery(luceneQuery, Entity.class);
         int numEntries = getNumEntries();
         while (id <= numEntries) {
            long start = System.nanoTime();
            q.list();
            latencyStats.recordLatency(System.nanoTime() - start);
            id = globalCounter.get();
            LockSupport.parkNanos(QUERY_INTERVAL_MS * 1000_000L);
         }
      }

      @Override
      void warmup() {
         for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            CacheQuery query = Search.getSearchManager(cache).getQuery(new MatchAllDocsQuery(), Entity.class);
            query.list();
         }
      }

   }

   class TimeBoundQueryNode extends QueryingNode {

      private final long durationNanos;
      private final long waitIntervalNanos;

      TimeBoundQueryNode(long totalTime, TimeUnit totalTimeUnit, long waitBetweenQueries, TimeUnit waiTimeUnit,
                         int nThreads, QueryType queryType) {
         super(nThreads, null, queryType);
         this.waitIntervalNanos = TimeUnit.NANOSECONDS.convert(waitBetweenQueries, waiTimeUnit);
         this.durationNanos = TimeUnit.NANOSECONDS.convert(totalTime, totalTimeUnit);
      }

      @Override
      protected int getRandomTerm() {
         return random.nextInt(getNumEntries());
      }

      @Override
      void executeTask() {
         long now = System.nanoTime();
         long timeLimit = now + durationNanos;
         Query luceneQuery = createLuceneQuery();
         CacheQuery q = Search.getSearchManager(cache).getQuery(luceneQuery, Entity.class);
         while (timeLimit - System.nanoTime() > 0L) {
            long start = System.nanoTime();
            q.list();
            latencyStats.recordLatency(System.nanoTime() - start);
            LockSupport.parkNanos(waitIntervalNanos);
         }
      }
   }

   class NodeSummary {
      private final Histogram histogram;
      private final long totalTimeMs;

      NodeSummary(Histogram histogram, long totalTimeMs) {
         this.histogram = histogram;
         this.totalTimeMs = totalTimeMs;
      }

      double getOpsPerSecond() {
         return histogram.getTotalCount() / (totalTimeMs / 1.0E3);
      }

      double getValueAtPercentile(int percentile) {
         return histogram.getValueAtPercentile(percentile) / 1.0E6;
      }

      void outputHistogram() {
         histogram.outputPercentileDistribution(System.out, 1000000.0);
      }

   }
}
