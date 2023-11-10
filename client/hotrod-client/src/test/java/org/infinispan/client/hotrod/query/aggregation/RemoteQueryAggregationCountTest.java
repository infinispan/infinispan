package org.infinispan.client.hotrod.query.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.AGGREGATION_RESULT;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.CHUNK_SIZE;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.NUMBER_OF_DAYS;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.REV_AGGREGATION_RESULT;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.chunk;

import java.util.Optional;
import java.util.Random;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
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

      return TestCacheManagerFactory.createServerModeCacheManager(indexed);
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

      QueryFactory queryFactory = Search.getQueryFactory(remoteCache);
      Query<Object[]> query;

      query = queryFactory.create("select status, count(code) from Sale where day = :day group by status order by status");
      query.setParameter("day", NUMBER_OF_DAYS / 2);
      assertThat(query.list()).containsExactly(AGGREGATION_RESULT);

      query = queryFactory.create("select count(code), status from Sale where day = :day group by status order by status");
      query.setParameter("day", NUMBER_OF_DAYS / 2);
      assertThat(query.list()).containsExactly(REV_AGGREGATION_RESULT);

      query = queryFactory.create("select status, count(code) from Sale where day = :day group by status");
      query.setParameter("day", NUMBER_OF_DAYS / 2);
      assertThat(query.list()).containsExactlyInAnyOrder(AGGREGATION_RESULT);

      query = queryFactory.create("select status, count(code) from Sale group by status");
      Optional<Integer> totalNotNullItems = query.list().stream()
            .map(objects -> ((Long) objects[1]).intValue()).reduce(Integer::sum);
      assertThat(totalNotNullItems).hasValue(CHUNK_SIZE * NUMBER_OF_DAYS);
   }
}
