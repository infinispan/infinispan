package org.infinispan.query.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.AGGREGATION_RESULT;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.CHUNK_SIZE;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.END_DAY;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.FULL_AGGREGATION_RESULT;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.NUMBER_OF_DAYS;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.REV_AGGREGATION_RESULT;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.START_DAY;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.chunk;

import java.util.Optional;
import java.util.Random;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.model.Sale;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.aggregation.QueryAggregationBroadcastTest")
public class QueryAggregationBroadcastTest extends MultipleCacheManagersTest {

   private final Random fixedSeedPseudoRandom = new Random(739);

   private Cache<Object, Object> cache;

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder indexed = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Sale.class);

      createClusteredCaches(8, Sale.SaleSchema.INSTANCE, indexed);
      cache = cache(0);
   }

   @Test
   public void test() {
      for (int day = 1; day <= NUMBER_OF_DAYS; day++) {
         cache.putAll(chunk(day, fixedSeedPseudoRandom));
      }

      Query<Object[]> query;

      query = cache.query("select status, count(code) from org.infinispan.query.model.Sale where day >= :start and day <= :end group by status order by status");
      query.setParameter("start", START_DAY);
      query.setParameter("end", END_DAY);
      assertThat(query.list()).containsExactly(AGGREGATION_RESULT);

      query = cache.query("select count(code), status from org.infinispan.query.model.Sale where day >= :start and day <= :end group by status order by status");
      query.setParameter("start", START_DAY);
      query.setParameter("end", END_DAY);
      assertThat(query.list()).containsExactly(REV_AGGREGATION_RESULT);

      query = cache.query("select status, count(code) from org.infinispan.query.model.Sale where day >= :start and day <= :end group by status");
      query.setParameter("start", START_DAY);
      query.setParameter("end", END_DAY);
      assertThat(query.list()).containsExactlyInAnyOrder(AGGREGATION_RESULT);

      query = cache.query("select status, count(code) from org.infinispan.query.model.Sale group by status");
      Optional<Integer> totalNotNullItems = query.list().stream()
            .map(objects -> ((Long) objects[1]).intValue()).reduce(Integer::sum);
      assertThat(totalNotNullItems).hasValue(CHUNK_SIZE * NUMBER_OF_DAYS);

      // alias
      query = cache.query("select s.status, count(s.code) from org.infinispan.query.model.Sale s where s.day >= :start and s.day <= :end group by s.status order by s.status");
      query.setParameter("start", START_DAY);
      query.setParameter("end", END_DAY);
      assertThat(query.list()).containsExactly(AGGREGATION_RESULT);
      // alias && count on entity
      query = cache.query("select s.status, count(s) from org.infinispan.query.model.Sale s where s.day >= :start and s.day <= :end group by s.status order by s.status");
      query.setParameter("start", START_DAY);
      query.setParameter("end", END_DAY);
      assertThat(query.list()).containsExactly(FULL_AGGREGATION_RESULT);
      // no alias && count on entity
      query = cache.query("select status, count(*) from org.infinispan.query.model.Sale where day >= :start and day <= :end group by status");
      query.setParameter("start", START_DAY);
      query.setParameter("end", END_DAY);
      assertThat(query.list()).containsExactly(FULL_AGGREGATION_RESULT);
   }
}
