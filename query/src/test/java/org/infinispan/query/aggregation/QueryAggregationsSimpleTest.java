package org.infinispan.query.aggregation;

import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.IndexedPlayer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

@Test(groups = "functional", testName = "query.aggregation.QueryAggregationSumTest")
public class QueryAggregationsSimpleTest extends SingleCacheManagerTest {

   // these results depend on the seed
   public static final Object[][] AGG_RESULT_SUM = {{"PINK", 100}};
   public static final Object[][] AGG_RESULT_AVG = {{"PINK", 50}};
   public static final Object[][] AGG_RESULT_COUNT = {{"PINK", 3L}};
   public static final Object[][] AGG_RESULT_COUNT_NUMBER = {{"PINK", 2L}};
   public static final Object[][] AGG_RESULT_MAX = {{"PINK", 56}};
   public static final Object[][] AGG_RESULT_MIN = {{"PINK", 44}};

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(IndexedPlayer.class);

      return TestCacheManagerFactory.createCacheManager(indexed);
   }

   @Test
   public void testSum() {
      cache.putAll(players());
      Query<Object[]> query;

      query = cache.query("select color, sum(number) from org.infinispan.query.model.IndexedPlayer group by color");
      assertThat(query.list()).containsExactly(AGG_RESULT_SUM);
   }

    @Test
    public void testCount() {
        cache.putAll(players());
        cache.put("3", new IndexedPlayer("player 3", Colors.PINK.name(), null));

        Query<Object[]> query = cache.query("select color, count(*) from org.infinispan.query.model.IndexedPlayer group by color");
        assertThat(query.list()).containsExactly(AGG_RESULT_COUNT);

        query = cache.query("select color, count(number) from org.infinispan.query.model.IndexedPlayer group by color");
        assertThat(query.list()).containsExactly(AGG_RESULT_COUNT_NUMBER);
    }

    @Test
    public void testAvg() {
        cache.putAll(players());
        Query<Object[]> query = cache.query("select color, avg(number) from org.infinispan.query.model.IndexedPlayer group by color");
        assertThat(query.list()).containsExactly(AGG_RESULT_AVG);
    }

    @Test
    public void testMax() {
        cache.putAll(players());
        Query<Object[]> query = cache.query("select color, max(number) from org.infinispan.query.model.IndexedPlayer group by color");
        assertThat(query.list()).containsExactly(AGG_RESULT_MAX);
    }

    @Test
    public void testMin() {
        cache.putAll(players());
        Query<Object[]> query = cache.query("select color, min(number) from org.infinispan.query.model.IndexedPlayer group by color");
        assertThat(query.list()).containsExactly(AGG_RESULT_MIN);
    }

   public static Map<String, IndexedPlayer> players() {
       Map<String, IndexedPlayer> players = new HashMap<>();
       players.put("1", new IndexedPlayer("player 1", Colors.PINK.name(), 56));
       players.put("2", new IndexedPlayer("player 2", Colors.PINK.name(), 44));
       return players;
   }

   public enum  Colors {
      PINK, BLUE, RED, YELLOW
   }
}
