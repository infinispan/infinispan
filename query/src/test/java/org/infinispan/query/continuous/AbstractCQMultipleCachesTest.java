package org.infinispan.query.continuous;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Map;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.Search;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Base class for continuous query tests with multiple caches.
 *
 * @author vjuranek
 * @since 8.0
 */
@Test(groups = "functional")
public abstract class AbstractCQMultipleCachesTest extends MultipleCacheManagersTest {

   protected final int NUM_NODES = 3;
   protected final int NUM_OWNERS = NUM_NODES - 1;

   protected abstract CacheMode getCacheMode();

   public AbstractCQMultipleCachesTest() {
      cleanup =  CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder c = buildConfiguration();
      createCluster(QueryTestSCI.INSTANCE, c, NUM_NODES);
      waitForClusterToForm();
   }

   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(getCacheMode(), false);
      c.clustering().hash().numOwners(NUM_OWNERS);
      return c;
   }

   protected CallCountingCQResultListener<Integer, Person> createContinuousQuery() {
      QueryFactory qf = Search.getQueryFactory(cache(0));

      Query<Person> query = qf.create("FROM org.infinispan.query.test.Person WHERE age <= 30");

      CallCountingCQResultListener<Integer, Person> listener = new CallCountingCQResultListener<>();
      ContinuousQuery<Integer, Person> cq = Search.getContinuousQuery(cache(0));
      cq.addContinuousQueryListener(query, listener);
      return listener;
   }

   public void testContinuousQueryMultipleCaches() {
      for (int i = 0; i < 2; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(22);
         cache(i).put(i, value);
      }

      CallCountingCQResultListener<Integer, Person> listener = createContinuousQuery();
      final Map<Integer, Integer> joined = listener.getJoined();
      final Map<Integer, Integer> left = listener.getLeft();

      assertEquals(2, joined.size());
      assertEquals(0, left.size());
      joined.clear();

      for (int i = 0; i < 2; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(40);
         cache(i).put(i, value);
      }

      assertEquals(0, joined.size());
      assertEquals(2, left.size());
      left.clear();

      for (int i = 0; i < 10; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);
         cache(0).put(i, value);
      }

      assertEquals(6, joined.size());
      assertEquals(0, left.size());
      for (int i = 0; i < 6; i++) {
         assertEquals(1, joined.get(i).intValue());
      }
      joined.clear();

      cache(0).clear();

      assertEquals(0, joined.size());
      assertEquals(6, left.size());
      for (int i = 0; i < 6; i++) {
         assertEquals(1, left.get(i).intValue());
      }
      left.clear();
   }

   public void testCQCacheLeavesAndJoins() {
      CallCountingCQResultListener<Integer, Person> listener = createContinuousQuery();
      final Map<Integer, Integer> joined = listener.getJoined();
      final Map<Integer, Integer> left = listener.getLeft();

      assertEquals(0, joined.size());
      assertEquals(0, left.size());

      for (int i = 0; i < 2; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(40);
         cache(i).put(i, value);
      }

      assertEquals(0, joined.size());
      assertEquals(0, left.size());

      for (int i = 0; i < 10; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);
         cache(0).put(i, value);
         if (i == 2) {
            // The listener matches entries with age <= 30
            // It will receive entries 0..2 before the node is killed, and entries 3..5 after
            killMember(1);
         }
      }

      assertEquals(6, joined.size());
      assertEquals(0, left.size());
      for (int i = 0; i < 6; i++) {
         assertEquals(1, joined.get(i).intValue());
      }
      joined.clear();

      cache(0).clear();
      assertEquals(0, joined.size());
      assertEquals(6, left.size());
      left.clear();

      for (int i = 0; i < 10; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);
         cache(0).put(i, value);
         if (i == 2) {
            // The listener matches entries with age <= 30
            // It will receive entries 0..2 before the node joins, and entries 3..5 after
            addClusterEnabledCacheManager(QueryTestSCI.INSTANCE, buildConfiguration());
         }
      }

      assertEquals(6, joined.size());
      assertEquals(0, left.size());
      for (int i = 0; i < 6; i++) {
         assertEquals(1, joined.get(i).intValue());
      }
      joined.clear();
   }
}
