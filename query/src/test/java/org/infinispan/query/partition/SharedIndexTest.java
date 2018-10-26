package org.infinispan.query.partition;

import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESMETADATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_LOCKING_CACHENAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.indexmanager.InfinispanIndexManager;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

/**
 * @since 9.3
 */
@Test(groups = "functional", testName = "query.partitionhandling.SharedIndexTest")
public class SharedIndexTest extends BasePartitionHandlingTest {

   protected int totalEntries = 100;

   public SharedIndexTest() {
      numMembersInCluster = 3;
      cacheMode = CacheMode.DIST_SYNC;
      cleanup = CleanupPhase.AFTER_TEST;
   }

   @Override
   protected ConfigurationBuilder cacheConfiguration() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.indexing()
            .index(Index.PRIMARY_OWNER)
            .addProperty("default.indexmanager", InfinispanIndexManager.class.getName());
      return configurationBuilder;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      postConfigure(cacheManagers);
   }

   protected void postConfigure(List<EmbeddedCacheManager> cacheManagers) {
      ConfigurationBuilder replBuilder = new ConfigurationBuilder();
      replBuilder.clustering().cacheMode(CacheMode.REPL_SYNC).partitionHandling()
            .whenSplit(PartitionHandling.DENY_READ_WRITES)
            .indexing().index(Index.NONE);
      Configuration replConfig = replBuilder.build();

      ConfigurationBuilder distBuilder = new ConfigurationBuilder();
      distBuilder.clustering().cacheMode(CacheMode.DIST_SYNC).partitionHandling()
            .whenSplit(PartitionHandling.DENY_READ_WRITES)
            .indexing().index(Index.NONE);
      Configuration distConfig = distBuilder.build();

      cacheManagers.forEach(cm -> {
         cm.defineConfiguration(DEFAULT_LOCKING_CACHENAME, replConfig);
         cm.defineConfiguration(DEFAULT_INDEXESDATA_CACHENAME, distConfig);
         cm.defineConfiguration(DEFAULT_INDEXESMETADATA_CACHENAME, replConfig);
      });
   }

   @Test(expectedExceptions = AvailabilityException.class)
   public void shouldThrowExceptionInDegradedMode() {
      Cache<Integer, Person> cache = cache(0);
      IntStream.range(0, totalEntries).forEach(i -> cache.put(i, new Person("Person " + i, "", i)));

      executeQueries();

      splitCluster(new int[]{0}, new int[]{1, 2});
      partition(0).assertDegradedMode();

      executeQueries();
   }

   protected void assertAllNodesQueryResults(int results) {
      assertEquals(results, totalEntries);
   }

   protected void assertSingleNodeQueryResults(int results) {
      assertTrue(results > 0);
   }

   private void executeQueries() {
      String q = getQuery();
      caches().forEach(c -> {
         Query allNodesQuery = Search.getQueryFactory(c).create(q, getIndexedQueryMode());
         assertAllNodesQueryResults(allNodesQuery.getResultSize());
      });
      Query singleNodeQuery = Search.getQueryFactory(cache(0)).create(q);
      assertSingleNodeQueryResults(singleNodeQuery.list().size());
   }

   protected IndexedQueryMode getIndexedQueryMode() {
      return IndexedQueryMode.FETCH;
   }

   protected String getQuery() {
      return "From " + Person.class.getName() + " p where p.name:'person*'";
   }
}
