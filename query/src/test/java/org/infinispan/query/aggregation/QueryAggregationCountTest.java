package org.infinispan.query.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.HashMap;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.model.Sale;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.aggregation.QueryAggregationCountTest")
public class QueryAggregationCountTest extends SingleCacheManagerTest {

   public static final int NUMBER_OF_DAYS = 10;
   public static final int CHUNK_SIZE = 1000;

   // these results depend on the seed
   public static final Object[][] AGGREGATION_RESULT = {{"BLOCKED", 161L}, {"CLOSE", 152L}, {"IN_PROGRESS", 174L}, {"OPEN", 141L}, {"WAITING", 172L}};
   public static final Object[][] REV_AGGREGATION_RESULT = {{161L, "BLOCKED"}, {152L, "CLOSE"}, {174L, "IN_PROGRESS"}, {141L, "OPEN"}, {172L, "WAITING"}};

   private final Random fixedSeedPseudoRandom = new Random(739);

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Sale.class);

      return TestCacheManagerFactory.createCacheManager(indexed);
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

   public static HashMap<String, Sale> chunk(int day, Random random) {
      HashMap<String, Sale> bulk = new HashMap<>();
      for (int ordinal = 0; ordinal < CHUNK_SIZE; ordinal++) {
         String id = String.format("%03d", day) + ":" + String.format("%03d", ordinal);
         Status status = Status.values()[random.nextInt(Status.values().length)];
         String code = (ordinal % Status.values().length == 0) ? null : UUID.randomUUID().toString();
         Sale sale = new Sale(id, code, status.name(), day);
         bulk.put(sale.getId(), sale);
      }
      return bulk;
   }

   public enum Status {
      OPEN, CLOSE, IN_PROGRESS, WAITING, BLOCKED
   }
}
