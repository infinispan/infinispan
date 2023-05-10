package org.infinispan.query.indexing;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.query.impl.config.SearchPropertyExtractor;
import org.infinispan.query.model.TypeA;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.indexing.DefaultIndexPathTest")
@TestForIssue(jiraKey = "ISPN-14109")
public class DefaultIndexPathTest extends SingleCacheManagerTest {

   private static final String CACHE_1_NAME = "cache-1";
   private static final String CACHE_2_NAME = "cache-2";
   private static final int SIZE = 400;

   private Path indexLocation1;
   private Path indexLocation2;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config
            .indexing()
            .enable()
            .storage(IndexStorage.FILESYSTEM) // use filesystem storage without defining a path
            .addIndexedEntity(TypeA.class);

      EmbeddedCacheManager result = TestCacheManagerFactory.createCacheManager();
      result.defineConfiguration(CACHE_1_NAME, config.build());
      result.defineConfiguration(CACHE_2_NAME, config.build());

      GlobalConfiguration globalConfiguration = result.getGlobalComponentRegistry().getGlobalConfiguration();
      indexLocation1 = SearchPropertyExtractor.getIndexLocation(globalConfiguration, null, CACHE_1_NAME);
      indexLocation2 = SearchPropertyExtractor.getIndexLocation(globalConfiguration, null, CACHE_2_NAME);
      return result;
   }

   @Test
   public void test() throws Exception {
      Map<String, TypeA> entries = IntStream.range(0, SIZE).boxed()
            .map(i -> "simple-" + i)
            .collect(Collectors.toMap(id -> "key-" + id, id -> new TypeA("value-" + id)));

      Cache<String, TypeA> cache1 = cacheManager.getCache(CACHE_1_NAME);
      Cache<String, TypeA> cache2 = cacheManager.getCache(CACHE_2_NAME);

      CompletableFuture<Void> future1 = cache1.putAllAsync(entries);
      CompletableFuture<Void> future2 = cache2.putAllAsync(entries);

      future1.get();
      // if the default filesystem storage is not chosen wisely, we're expecting a lock error such as:
      //
      // java.util.concurrent.ExecutionException: org.infinispan.commons.CacheException:
      // HSEARCH600016: Unable to index entity of type 'org.infinispan.query.model.TypeA' with identifier 'key-simple-313' and tenant identifier 'null':
      // Lock held by this virtual machine: /Users/fabio/code/infinispan/query/index-A/write.lock
      // Context: index 'index-A'
      future2.get();

      CompletionStage<Map<String, IndexInfo>> infoIndex1 = Search.getSearchStatistics(cache1).getIndexStatistics()
            .computeIndexInfos();
      CompletionStage<Map<String, IndexInfo>> infoIndex2 = Search.getSearchStatistics(cache2).getIndexStatistics()
            .computeIndexInfos();

      infoIndex1.toCompletableFuture().get();

      // same here:
      infoIndex2.toCompletableFuture().get();
   }

   @Override
   protected void teardown() {
      // index-A is specified as index in TypeA class >> @Indexed(index = "index-A")
      Util.recursiveFileRemove(indexLocation1);
      Util.recursiveFileRemove(indexLocation2);
      super.teardown();
   }
}
