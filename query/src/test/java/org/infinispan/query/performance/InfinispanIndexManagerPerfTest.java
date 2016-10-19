package org.infinispan.query.performance;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.IntStream.range;
import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.configuration.cache.CacheMode.REPL_SYNC;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.indexmanager.InfinispanIndexManager;
import org.infinispan.query.test.Person;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * @since 9.0
 */
@Test(groups = "stress", testName = "query.performance.InfinispanIndexManagerPerfTest")
public class InfinispanIndexManagerPerfTest extends MultipleCacheManagersTest {

   private static final int THREADS_PER_NODE = 10;
   private static final int NUM_NODES = 2;
   private static final int NUM_ENTRIES = 50000;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(DIST_SYNC, false);
      cacheCfg.clustering().remoteTimeout(1L, TimeUnit.MINUTES)
            .indexing()
            .index(Index.LOCAL)
            .addIndexedEntity(Person.class)
            .addProperty("default.indexmanager", InfinispanIndexManager.class.getName())
            .addProperty("default.worker.execution", "async")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler");

      range(0, NUM_NODES).forEach(n -> {
         EmbeddedCacheManager cacheManager = addClusterEnabledCacheManager(cacheCfg);
         defineIndexConfiguration(cacheManager);
      });

      waitForClusterToForm();
   }

   @Test
   public void testPerf() throws Exception {
      AtomicInteger counter = new AtomicInteger(0);
      ExecutorService executorService = Executors.newFixedThreadPool(THREADS_PER_NODE * NUM_NODES);

      Stream<CompletableFuture<?>> futureStream = caches().stream().parallel().flatMap(cache -> {
         Address address = cache.getAdvancedCache().getRpcManager().getAddress();
         List<CompletableFuture<?>> futures = range(0, THREADS_PER_NODE).boxed().map(t -> supplyAsync(() -> {
            int nextId = 0;
            while (nextId <= NUM_ENTRIES) {
               nextId = counter.incrementAndGet();
               if (nextId <= NUM_ENTRIES) {
                  cache.put(nextId, new Person("name" + nextId, "blurb" + nextId, 0));
                  System.out.println("Put " + nextId + " for address " + address + " from " + Thread.currentThread().getName());
               }
            }
            return null;
         }, executorService)).collect(Collectors.toList());
         return futures.stream();
      });

      futureStream.forEach(CompletableFuture::join);

      eventually(() -> {
         int indexedSize = Search.getSearchManager(cache(0)).getQuery(new MatchAllDocsQuery(), Person.class).getResultSize();
         return indexedSize == NUM_ENTRIES;
      });

      caches().forEach(StaticTestingErrorHandler::assertAllGood);
   }

   @Override
   protected EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilder builder, TransportFlags flags) {
      GlobalConfigurationBuilder globalConfigurationBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      TestCacheManagerFactory.amendTransport(globalConfigurationBuilder);
      EmbeddedCacheManager manager = TestCacheManagerFactory.newDefaultCacheManager(true, globalConfigurationBuilder, builder, false);
      cacheManagers.add(manager);
      return manager;
   }

   private void defineIndexConfiguration(EmbeddedCacheManager cm) {
      cm.defineConfiguration("LuceneIndexesMetadata", getIndexCacheConfig(REPL_SYNC));
      cm.defineConfiguration("LuceneIndexesLocking", getIndexCacheConfig(REPL_SYNC));
      cm.defineConfiguration("LuceneIndexesData", getIndexCacheConfig(DIST_SYNC));
   }

   private Configuration getIndexCacheConfig(CacheMode cacheMode) {
      return new ConfigurationBuilder().clustering().remoteTimeout(1, TimeUnit.MINUTES).cacheMode(cacheMode).indexing().index(Index.NONE).build();
   }
}
