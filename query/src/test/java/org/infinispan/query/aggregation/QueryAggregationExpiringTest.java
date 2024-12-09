package org.infinispan.query.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.Task;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.aggregation.QueryAggregationExpiringTest")
public class QueryAggregationExpiringTest extends SingleCacheManagerTest {

   private final ControlledTimeService timeService = new ControlledTimeService();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.statistics().enable();
      config.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Task.class);

      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(config);
      TestingUtil.replaceComponent(manager, TimeService.class, timeService, true);
      return manager;
   }

   @Test
   public void test() throws Exception {
      for (int i=0; i<12; i++) {
         int module = i % 4;
         cache.put(i, new Task(100 + i, "type-" + module, "status-" + module, "label-" + module),
               1, TimeUnit.SECONDS);
         timeService.advance(10, TimeUnit.MILLISECONDS);
      }

      timeService.advance(500, TimeUnit.MILLISECONDS);
      Map<String, Long> result;
      do {
         timeService.advance(100, TimeUnit.MILLISECONDS);
         Query<Object[]> query = cache.query("select status, count(status) from org.infinispan.query.model.Task group by status");
         result = resultMaps(query.list());
         assertThat(result).isNotNull();

         query = cache.query("select label, count(label) from org.infinispan.query.model.Task group by label");
         result = resultMaps(query.list());
         assertThat(result).isNotNull();

         query = cache.query("select type, count(type) from org.infinispan.query.model.Task group by type");
         result = resultMaps(query.list());
         assertThat(result).isNotNull();
      } while (!result.isEmpty());
   }

   public static Map<String, Long> resultMaps(List<Object[]> list) {
      return list.stream().collect(Collectors.toMap(
            i -> (String) i[0], i -> (Long) i[1]
      ));
   }
}
