package org.infinispan.query.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.sampledomain.NumericAggregation;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.aggregation.QueryAverageTest")
public class QueryAverageTest extends MultipleCacheManagersTest {
   private boolean indexed;

   @Override
   public Object[] factory() {
      return new Object[] {
            new QueryAverageTest().configuration(CacheMode.LOCAL, true),
            new QueryAverageTest().configuration(CacheMode.LOCAL, false)
      };
   }

   private QueryAverageTest configuration(CacheMode mode, boolean indexed) {
      cacheMode = mode;
      this.indexed = indexed;
      return this;
   }

   @Override
   protected String[] parameterNames() {
      return new String[] {"cacheMode", "indexed"};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[] {cacheMode, indexed};
   }

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode);
      if (indexed) {
         builder.indexing().enable().storage(LOCAL_HEAP).addIndexedEntity(NumericAggregation.class);
      }
      createClusteredCaches(1, NumericAggregation.NumericAggregationSchema.INSTANCE, builder);
   }

   public void testAverage() {
      put(0, 1);
      put(1, 1);
      put(2, 2);
      put(3, 2);
      cache(0).put("null", new NumericAggregation("values", null, null));
      cache(0).put("empty", new NumericAggregation("empty", null, null));
      for (String field : new String[] {"integerValue", "longValue"}) {
         String from = " from " + NumericAggregation.class.getName();
         Query<Object[]> single = cache(0).query("select category, avg(" + field + ")" + from +
               " group by category order by category");
         assertThat(single.list()).containsExactly(new Object[] {"empty", null}, new Object[] {"values", 1.5d});
         assertThat(single.list().get(1)[1]).isInstanceOf(Double.class);
         Query<Object[]> multiple = cache(0).query("select category, count(" + field + "), avg(" + field + ")" + from +
               " group by category having avg(" + field + ") > 1");
         assertThat(multiple.list()).containsExactly(new Object[] {"values", 4L, 1.5d});
         Query<Object[]> ungrouped = cache(0).query("select avg(" + field + ")" + from);
         assertThat(ungrouped.list()).containsExactly(new Object[] {1.5d});
      }
   }

   private void put(int index, int value) {
      String key = "key" + index;
      cache(0).put(key, new NumericAggregation("values", value, (long) value));
   }
}
