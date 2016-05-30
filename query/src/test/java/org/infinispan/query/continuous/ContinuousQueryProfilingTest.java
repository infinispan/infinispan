package org.infinispan.query.continuous;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.Search;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.api.continuous.ContinuousQueryListener;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.test.Person;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 8.2
 */
@Test(groups = "profiling", testName = "query.continuous.ContinuousQueryProfilingTest")
public class ContinuousQueryProfilingTest extends MultipleCacheManagersTest {

   private final int NUM_NODES = 10;

   private final int NUM_OWNERS = 3;

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder c = buildConfiguration();
      createCluster(c, NUM_NODES);
      waitForClusterToForm();
   }

   private ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      c.clustering().hash().numOwners(NUM_OWNERS);
      return c;
   }

   public void testContinuousQueryPerformance() {
      long t1 = testContinuousQueryPerformance(false);
      long t2 = testContinuousQueryPerformance(true);
      log.infof("ContinuousQueryProfilingTest.testContinuousQueryPerformance doRegisterListener=false took %d ms\n", t1 / 1000000);
      log.infof("ContinuousQueryProfilingTest.testContinuousQueryPerformance doRegisterListener=true  took %d ms\n", t2 / 1000000);
   }

   private long testContinuousQueryPerformance(boolean doRegisterListener) {
      final int numEntries = 100000;
      final int numListeners = 1000;
      ContinuousQuery<Object, Object> cq = Search.getContinuousQuery(cache(0));
      if (doRegisterListener) {
         Query query = makeQuery(cache(0));
         for (int i = 0; i < numListeners; i++) {
            cq.addContinuousQueryListener(query, new ContinuousQueryListener<Object, Object>() {
            });
         }
      }

      long startTs = System.nanoTime();
      // create entries
      for (int i = 0; i < numEntries; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);

         Cache<Object, Person> cache = cache(i % NUM_NODES);
         cache.put(value.getName(), value);
      }
      // update entries (with same value)
      for (int i = 0; i < numEntries; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);

         Cache<Object, Person> cache = cache(i % NUM_NODES);
         cache.put(value.getName(), value);
      }
      long endTs = System.nanoTime();

      cq.removeAllListeners();

      return endTs - startTs;
   }

   private Query makeQuery(Cache c) {
      QueryFactory qf = Search.getQueryFactory(c);
      return qf.from(Person.class)
            .having("age").gte(18)
            .toBuilder().build();
   }
}
