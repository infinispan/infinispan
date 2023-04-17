package org.infinispan.query.reindex;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.util.concurrent.CompletionStages.join;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.cache.IndexingMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Indexer;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.query.core.stats.SearchStatistics;
import org.infinispan.query.model.TypeA;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.reindex.LocalIndexerRemoveTest")
@TestForIssue(jiraKey = "ISPN-14189")
public class LocalIndexerRemoveTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "types";
   private static final int ENTRIES = 5_000;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config
            .indexing()
            .enable()
            .indexingMode(IndexingMode.MANUAL)
            .storage(IndexStorage.LOCAL_HEAP)
            .addIndexedEntity(TypeA.class);

      EmbeddedCacheManager result = TestCacheManagerFactory.createCacheManager();
      result.defineConfiguration(CACHE_NAME, config.build());

      return result;
   }

   @Test
   public void test() {
      Cache<Integer, TypeA> typesCache = cacheManager.getCache(CACHE_NAME);
      Indexer indexer = Search.getIndexer(typesCache);
      SearchStatistics searchStatistics = Search.getSearchStatistics(typesCache);

      Map<Integer, TypeA> values = IntStream.range(0, ENTRIES).boxed()
            .collect(Collectors.toMap(Function.identity(), i -> new TypeA("value " + i)));
      typesCache.putAll(values);

      // the indexing mode is manual, thus the index is empty
      assertThat(count(searchStatistics)).isZero();

      join(indexer.runLocal());

      // the indexer fills the index
      assertThat(count(searchStatistics)).isEqualTo(ENTRIES);

      join(indexer.remove());

      // removing the index data
      assertThat(count(searchStatistics)).isZero();
   }

   private long count(SearchStatistics searchStatistics) {
      Map<String, IndexInfo> indexInfo = join(searchStatistics.getIndexStatistics().computeIndexInfos());
      String key = TypeA.class.getName();
      assertThat(indexInfo).containsKey(key);
      return indexInfo.get(key).count();
   }
}
