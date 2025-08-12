package org.infinispan.client.hotrod.query.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.AGGREGATION_RESULT;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.END_DAY;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.FULL_AGGREGATION_RESULT;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.NUMBER_OF_DAYS;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.REV_AGGREGATION_RESULT;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.START_DAY;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.TOTAL_NOT_NULL_ITEMS;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.chunk;

import java.util.Optional;
import java.util.Random;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.model.Sale;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.aggregation.RemoteQueryAggregationCountTest")
public class RemoteQueryAggregationCountTest extends SingleHotRodServerTest {

   private final Random fixedSeedPseudoRandom = new Random(739);

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("Sale");

      return TestCacheManagerFactory.createServerModeCacheManager(contextInitializer(), indexed);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Sale.SaleSchema.INSTANCE;
   }

   @Test
   public void test() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      for (int day = 1; day <= NUMBER_OF_DAYS; day++) {
         remoteCache.putAll(chunk(day, fixedSeedPseudoRandom));
      }

      Query<Object[]> query;

      query = remoteCache.query("select status, count(code) from Sale where day >= :start and day <= :end group by status order by status");
      query.setParameter("start", START_DAY);
      query.setParameter("end", END_DAY);
      assertThat(query.list()).containsExactly(AGGREGATION_RESULT);

      query = remoteCache.query("select count(code), status from Sale where day >= :start and day <= :end group by status order by status");
      query.setParameter("start", START_DAY);
      query.setParameter("end", END_DAY);
      assertThat(query.list()).containsExactly(REV_AGGREGATION_RESULT);

      query = remoteCache.query("select status, count(code) from Sale where day >= :start and day <= :end group by status");
      query.setParameter("start", START_DAY);
      query.setParameter("end", END_DAY);
      assertThat(query.list()).containsExactlyInAnyOrder(AGGREGATION_RESULT);

      query = remoteCache.query("select status, count(code) from Sale group by status");
      Optional<Integer> totalNotNullItems = query.list().stream()
            .map(objects -> ((Long) objects[1]).intValue()).reduce(Integer::sum);
      assertThat(totalNotNullItems).hasValue(TOTAL_NOT_NULL_ITEMS);

      // alias
      query = remoteCache.query("select s.status, count(s.code) from Sale s where s.day >= :start and s.day <= :end group by s.status order by s.status");
      query.setParameter("start", START_DAY);
      query.setParameter("end", END_DAY);
      assertThat(query.list()).containsExactly(AGGREGATION_RESULT);
      // alias && count on entity
      query = remoteCache.query("select s.status, count(s) from Sale s where s.day >= :start and s.day <= :end group by s.status order by s.status");
      query.setParameter("start", START_DAY);
      query.setParameter("end", END_DAY);
      assertThat(query.list()).containsExactly(FULL_AGGREGATION_RESULT);
      // no alias && count on entity
      query = remoteCache.query("select status, count(*) from Sale where day >= :start and day <= :end group by status");
      query.setParameter("start", START_DAY);
      query.setParameter("end", END_DAY);
      assertThat(query.list()).containsExactly(FULL_AGGREGATION_RESULT);
   }
}
