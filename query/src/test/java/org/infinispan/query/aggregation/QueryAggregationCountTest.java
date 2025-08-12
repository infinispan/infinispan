package org.infinispan.query.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.HashMap;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.Sale;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.aggregation.QueryAggregationCountTest")
public class QueryAggregationCountTest extends SingleCacheManagerTest {

   public static final int TOTAL_NOT_NULL_ITEMS = 800;
   public static final int NUMBER_OF_DAYS = 100;
   public static final int CHUNK_SIZE = 10;

   // these results depend on the seed
   public static final Object[][] AGGREGATION_RESULT = {{"BLOCKED", 16L}, {"CLOSE", 13L}, {"IN_PROGRESS", 22L}, {"OPEN", 20L}, {"WAITING", 9L}};
   public static final Object[][] REV_AGGREGATION_RESULT = {{16L, "BLOCKED"}, {13L, "CLOSE"}, {22L, "IN_PROGRESS"}, {20L, "OPEN"}, {9L, "WAITING"}};
   public static final Object[][] FULL_AGGREGATION_RESULT = {{"BLOCKED", 21L}, {"CLOSE", 16L}, {"IN_PROGRESS", 26L}, {"OPEN", 24L}, {"WAITING", 13L}};
   public static final int START_DAY = 45;
   public static final int END_DAY = 54;

   private final Random fixedSeedPseudoRandom = new Random(739);

   @Override
   protected EmbeddedCacheManager createCacheManager() {
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

      query = cache.query("select status, count(code) from org.infinispan.query.model.Sale where day >= :start and day <= :end group by status order by status");
      query.setParameter("start", START_DAY);
      query.setParameter("end", END_DAY);
      assertThat(query.list()).containsExactly(AGGREGATION_RESULT);
      // inverted count / group
      query = cache.query("select count(code), status from org.infinispan.query.model.Sale where day >= :start and day <= :end group by status order by status");
      query.setParameter("start", START_DAY);
      query.setParameter("end", END_DAY);
      assertThat(query.list()).containsExactly(REV_AGGREGATION_RESULT);
      // no order by
      query = cache.query("select status, count(code) from org.infinispan.query.model.Sale where day >= :start and day <= :end group by status");
      query.setParameter("start", START_DAY);
      query.setParameter("end", END_DAY);
      assertThat(query.list()).containsExactlyInAnyOrder(AGGREGATION_RESULT);

      query = cache.query("select status, count(code) from org.infinispan.query.model.Sale group by status");
      Optional<Integer> totalNotNullItems = query.list().stream()
            .map(objects -> ((Long) objects[1]).intValue()).reduce(Integer::sum);
      assertThat(totalNotNullItems).hasValue(TOTAL_NOT_NULL_ITEMS);

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
