package org.infinispan.query.partitionhandling;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
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
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class);
      return configurationBuilder;
   }

   @Override
   protected SerializationContextInitializer serializationContextInitializer() {
      return QueryTestSCI.INSTANCE;
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
         Query allNodesQuery = Search.getQueryFactory(c).create(q);
         assertAllNodesQueryResults(allNodesQuery.getResultSize());
      });
      Query singleNodeQuery = Search.getQueryFactory(cache(0)).create(q);
      assertSingleNodeQueryResults(singleNodeQuery.list().size());
   }

   protected String getQuery() {
      return "from " + Person.class.getName() + " p where p.name:'person*'";
   }
}
