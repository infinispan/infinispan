package org.infinispan.query.reindex;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.test.TestingUtil.join;

import java.util.Map;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexingMode;
import org.infinispan.configuration.cache.PrivateIndexingConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Indexer;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.query.core.stats.IndexStatistics;
import org.infinispan.query.model.Game;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.reindex.ReindexPressureTest")
public class ReindexPressureTest extends SingleCacheManagerTest {

   // the minimum value to reproduce the case on my machine is 900
   // if we cannot lower the rebatch requests size more,
   // see the next comment
   private static final int SIZE = 900;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      // the fix set the concurrency to 100,
      // so to reproduce the use case we need to use a rebatch-requests-size value that is grater than 100:
      config.addModule(PrivateIndexingConfigurationBuilder.class).rebatchRequestsSize(200);
      config.statistics().enable();
      config.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Game.class)
            .indexingMode(IndexingMode.MANUAL);
      return TestCacheManagerFactory.createCacheManager(config);
   }

   @Test
   public void smoke() throws Exception {
      for (int i = 0; i < SIZE; i++) {
         cache.put(i, new Game("name " + i, "description " + i));
      }

      IndexStatistics indexStatistics = Search.getSearchStatistics(cache).getIndexStatistics();
      Map<String, IndexInfo> indexInfos = join(indexStatistics.computeIndexInfos());
      assertThat(indexInfos.get(Game.class.getName())).extracting(IndexInfo::count).isEqualTo(0L);

      Indexer indexer = Search.getIndexer(cache);
      join(indexer.runLocal());

      indexInfos = join(indexStatistics.computeIndexInfos());
      assertThat(indexInfos.get(Game.class.getName())).extracting(IndexInfo::count).isEqualTo((long) SIZE);
   }
}
