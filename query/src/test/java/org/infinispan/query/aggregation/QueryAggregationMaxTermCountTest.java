package org.infinispan.query.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.Sale;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.aggregation.QueryAggregationMaxTermCountTest")
public class QueryAggregationMaxTermCountTest extends SingleCacheManagerTest {

   private static final int NUM_DISTINCT_STATUSES = 150;
   private static final int CONFIGURED_MAX_RESULTS = 200;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Sale.class);
      indexed.query().defaultMaxResults(CONFIGURED_MAX_RESULTS);

      return TestCacheManagerFactory.createCacheManager(indexed);
   }

   @BeforeMethod
   public void populateCache() {
      cache.clear();
      for (int i = 0; i < NUM_DISTINCT_STATUSES; i++) {
         String status = String.format("STATUS_%03d", i);
         String id = String.valueOf(i);
         cache.put(id, new Sale(id, "code-" + i, status, 1));
      }
   }

   @Test
   public void testCountAggregationReturnsMoreThanDefaultMaxTermCount() {
      Query<Object[]> query = cache.query(
            "select s.status, count(s.code) from org.infinispan.query.model.Sale s group by s.status");
      List<Object[]> results = query.list();
      assertThat(results).hasSize(NUM_DISTINCT_STATUSES);
   }

   @Test
   public void testCountStarAggregationReturnsMoreThanDefaultMaxTermCount() {
      Query<Object[]> query = cache.query(
            "select s.status, count(*) from org.infinispan.query.model.Sale s group by s.status");
      List<Object[]> results = query.list();
      assertThat(results).hasSize(NUM_DISTINCT_STATUSES);
   }
}
