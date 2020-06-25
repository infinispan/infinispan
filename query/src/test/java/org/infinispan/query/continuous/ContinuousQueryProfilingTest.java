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
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 8.2
 */
@Test(groups = "profiling", testName = "query.continuous.ContinuousQueryProfilingTest")
public class ContinuousQueryProfilingTest extends MultipleCacheManagersTest {

   private static final int NUM_NODES = 10;
   private static final int NUM_OWNERS = 3;
   private static final int NUM_ENTRIES = 100000;
   private static final int NUM_LISTENERS = 1000;

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder c = buildConfiguration();
      createCluster(QueryTestSCI.INSTANCE, c, NUM_NODES);
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
      log.infof("ContinuousQueryProfilingTest.testContinuousQueryPerformance doRegisterListener=false took %d us\n", t1 / 1000);
      log.infof("ContinuousQueryProfilingTest.testContinuousQueryPerformance doRegisterListener=true  took %d us\n", t2 / 1000);
   }

   private long testContinuousQueryPerformance(boolean doRegisterListener) {
      ContinuousQuery<String, Person> cq = Search.getContinuousQuery(cache(0));
      if (doRegisterListener) {
         Query<Person> query = makeQuery(cache(0));
         for (int i = 0; i < NUM_LISTENERS; i++) {
            cq.addContinuousQueryListener(query, new ContinuousQueryListener<String, Person>() {
            });
         }
      }

      long startTs = System.nanoTime();
      // create entries
      for (int i = 0; i < NUM_ENTRIES; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);

         Cache<String, Person> cache = cache(i % NUM_NODES);
         cache.put(value.getName(), value);
      }
      // update entries (with same value)
      for (int i = 0; i < NUM_ENTRIES; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);

         Cache<String, Person> cache = cache(i % NUM_NODES);
         cache.put(value.getName(), value);
      }
      long endTs = System.nanoTime();

      cq.removeAllListeners();

      return endTs - startTs;
   }

   private Query<Person> makeQuery(Cache<?, ?> c) {
      QueryFactory qf = Search.getQueryFactory(c);
      return qf.create("FROM org.infinispan.query.test.Person WHERE age >= 18");
   }
}
