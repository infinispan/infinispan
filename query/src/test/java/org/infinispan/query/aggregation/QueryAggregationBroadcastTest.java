package org.infinispan.query.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.AGGREGATION_RESULT;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.CHUNK_SIZE;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.NUMBER_OF_DAYS;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.REV_AGGREGATION_RESULT;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.chunk;

import java.util.Optional;
import java.util.Random;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.model.Sale;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.aggregation.QueryAggregationBroadcastTest")
public class QueryAggregationBroadcastTest extends MultipleCacheManagersTest {

   private final Random fixedSeedPseudoRandom = new Random(739);

   private Cache<Object, Object> cache;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder indexed = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Sale.class);

      createClusteredCaches(8, indexed);
      cache = cache(0);
   }

   @Test
   public void test() {
      for (int day = 1; day <= NUMBER_OF_DAYS; day++) {
         cache.putAll(chunk(day, fixedSeedPseudoRandom));
      }

      Query<Object[]> query;
      QueryFactory queryFactory = Search.getQueryFactory(cache);

      query = queryFactory.create("select status, count(code) from org.infinispan.query.model.Sale where day = :day group by status order by status");
      query.setParameter("day", NUMBER_OF_DAYS / 2);
      assertThat(query.list()).containsExactly(AGGREGATION_RESULT);

      query = queryFactory.create("select count(code), status from org.infinispan.query.model.Sale where day = :day group by status order by status");
      query.setParameter("day", NUMBER_OF_DAYS / 2);
      assertThat(query.list()).containsExactly(REV_AGGREGATION_RESULT);

      query = queryFactory.create("select status, count(code) from org.infinispan.query.model.Sale where day = :day group by status");
      query.setParameter("day", NUMBER_OF_DAYS / 2);
      assertThat(query.list()).containsExactlyInAnyOrder(AGGREGATION_RESULT);

      query = queryFactory.create("select status, count(code) from org.infinispan.query.model.Sale group by status");
      Optional<Integer> totalNotNullItems = query.list().stream()
            .map(objects -> ((Long) objects[1]).intValue()).reduce(Integer::sum);
      assertThat(totalNotNullItems).hasValue(CHUNK_SIZE * NUMBER_OF_DAYS);
   }
}
