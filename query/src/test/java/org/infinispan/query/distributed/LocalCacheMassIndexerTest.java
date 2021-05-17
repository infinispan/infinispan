package org.infinispan.query.distributed;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.junit.Assert.assertFalse;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Indexer;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for Mass Indexer in a local cache.
 *
 * @author gustavonalle
 * @since 7.1
 */

@Test(groups = "functional", testName = "query.distributed.LocalCacheMassIndexerTest")
public class LocalCacheMassIndexerTest extends SingleCacheManagerTest {

   private static final int NUM_ENTITIES = 50;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(false);
      cfg.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   private long indexSize(Cache<?, ?> cache) {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      // queryFactory.refresh(Object.class);

      Query<?> query = queryFactory.create("FROM " + Person.class.getName());
      return query.execute().hitCount().orElse(-1);
   }

   private void fillData() {
      for (int i = 0; i < NUM_ENTITIES; i++) {
         cache.put(i, new Person("name" + i, "blurb" + i, i));
      }
   }

   @BeforeMethod
   public void clean() {
      cache.clear();
   }

   @Test
   public void testMassIndexer() {
      fillData();
      Indexer massIndexer = Search.getIndexer(cache);

      assertEquals(NUM_ENTITIES, indexSize(cache));

      join(massIndexer.run());
      assertEquals(NUM_ENTITIES, indexSize(cache));

      cache.clear();
      join(massIndexer.run());

      assertEquals(0, indexSize(cache));

      fillData();
      join(massIndexer.run());
      assertFalse(massIndexer.isRunning());
      assertEquals(NUM_ENTITIES, indexSize(cache));

      // Force local
      join(massIndexer.runLocal());
      assertFalse(massIndexer.isRunning());
      assertEquals(NUM_ENTITIES, indexSize(cache));
   }

   public void testPartiallyReindex() throws Exception {
      cache.getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(0, new Person("name" + 0, "blurb" + 0, 0));
      verifyFindsPerson(0, "name" + 0);
      join(Search.getIndexer(cache).run(0));
      verifyFindsPerson(1, "name" + 0);
      cache.remove(0);
      verifyFindsPerson(0, "name" + 0);
   }

   protected void verifyFindsPerson(int expectedCount, String name) throws Exception {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      String q = String.format("FROM %s where name:'%s'", Person.class.getName(), name);
      Query cacheQuery = queryFactory.create(q);
      assertEquals(expectedCount, cacheQuery.execute().hitCount().orElse(-1));
   }
}
