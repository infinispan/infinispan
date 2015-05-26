package org.infinispan.query.distributed;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests for Mass Indexer in a local cache.
 *
 * @author gustavonalle
 * @since 7.1
 */

@Test(groups = "functional", testName = "query.distributed.LocalCacheMassIndexerTest")
public class LocalCacheMassIndexerTest extends SingleCacheManagerTest {

   private static final int NUM_ENTITIES = 2000;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(false);
      cfg.indexing().index(Index.ALL).addProperty("default.directory_provider", "ram");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   private int indexSize(Cache<?, ?> cache) {
      SearchManager searchManager = Search.getSearchManager(cache);
      return searchManager.getQuery(new MatchAllDocsQuery(), Person.class).getResultSize();
   }

   @Test
   public void testMassIndexer() {
      for (int i = 0; i < NUM_ENTITIES; i++) {
         cache.put(i, new Person("name" + i, "blurb" + i, i));
      }
      SearchManager searchManager = Search.getSearchManager(cache);

      assertEquals(NUM_ENTITIES, indexSize(cache));

      searchManager.getMassIndexer().start();
      assertEquals(NUM_ENTITIES, indexSize(cache));

      cache.clear();
      searchManager.getMassIndexer().start();

      assertEquals(0, indexSize(cache));
   }
}
