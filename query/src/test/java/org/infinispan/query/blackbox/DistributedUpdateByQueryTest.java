package org.infinispan.query.blackbox;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Tests clustered UPDATE BY QUERY across distributed caches.
 *
 * @since 16.3
 */
@Test(groups = "functional", testName = "query.blackbox.DistributedUpdateByQueryTest")
public class DistributedUpdateByQueryTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheCfg
            .clustering().hash().numOwners(1)
            .indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class);
      createClusteredCaches(2, QueryTestSCI.INSTANCE, cacheCfg);
   }

   @Test
   public void testDistributedUpdate() {
      IntStream.range(0, 10).forEach(i -> {
         Cache<String, Person> c = cache(i % 2);
         c.put("person" + i, new Person("name" + i, "blurb" + i, i));
      });

      Query<Person> update = cache(0).query(
            "UPDATE FROM " + Person.class.getName() + " SET blurb = 'updated' WHERE blurb : 'blurb1*'");
      int count = update.executeStatement();
      assertEquals(1, count);

      Person updated = (Person) cache(0).get("person1");
      assertEquals("updated", updated.getBlurb());
   }

   @Test
   public void testDistributedUpdateMultipleEntries() {
      IntStream.range(0, 10).forEach(i -> {
         Cache<String, Person> c = cache(i % 2);
         c.put("person" + i, new Person("testName", "blurb" + i, 42));
      });

      Query<Person> update = cache(0).query(
            "UPDATE FROM " + Person.class.getName() + " SET name = 'updatedName' WHERE age = 42");
      int count = update.executeStatement();
      assertEquals(10, count);

      for (int i = 0; i < 10; i++) {
         Person p = (Person) cache(i % 2).get("person" + i);
         assertEquals("updatedName", p.getName());
      }
   }

   @Test
   public void testDistributedUpdateWithParameter() {
      cache(0).put("p1", new Person("Alice", "blurb1", 30));
      cache(1).put("p2", new Person("Bob", "blurb2", 30));
      cache(0).put("p3", new Person("Charlie", "blurb3", 35));

      Query<Person> update = cache(0).query(
            "UPDATE FROM " + Person.class.getName() + " SET blurb = :newBlurb WHERE age = :targetAge");
      update.setParameter("newBlurb", "senior");
      update.setParameter("targetAge", 30);
      int count = update.executeStatement();
      assertEquals(2, count);

      assertEquals("senior", ((Person) cache(0).get("p1")).getBlurb());
      assertEquals("senior", ((Person) cache(1).get("p2")).getBlurb());
      assertEquals("blurb3", ((Person) cache(0).get("p3")).getBlurb());
   }

   @Test
   public void testDistributedUpdateWithArithmeticExpression() {
      cache(0).put("p1", new Person("Alice", "blurb1", 30));
      cache(1).put("p2", new Person("Bob", "blurb2", 31));

      Query<Person> update = cache(0).query(
            "UPDATE FROM " + Person.class.getName() + " SET age = age + 5 WHERE age = 30");
      int count = update.executeStatement();
      assertEquals(1, count);

      assertEquals(35, ((Person) cache(0).get("p1")).getAge());
      assertEquals(31, ((Person) cache(1).get("p2")).getAge());
   }

   @Test
   public void testDistributedUpdateWithFunctionExpression() {
      cache(0).put("p1", new Person("Alice", "blurb1", 30));
      cache(1).put("p2", new Person("Bob", "blurb2", 31));

      Query<Person> update = cache(0).query(
            "UPDATE FROM " + Person.class.getName() + " SET name = upper(name) WHERE age = 30");
      int count = update.executeStatement();
      assertEquals(1, count);

      assertEquals("ALICE", ((Person) cache(0).get("p1")).getName());
      assertEquals("Bob", ((Person) cache(1).get("p2")).getName());
   }

   @Test
   public void testDistributedUpdateWithDivideFunction() {
      cache(0).put("p1", new Person("Alice", "blurb1", 90));
      cache(1).put("p2", new Person("Bob", "blurb2", 31));

      // Repeated division is only expressible with divide() calls, not with
      // '/' operators: divide(divide(age, 2), 3) == age / 2 / 3.
      Query<Person> update = cache(0).query(
            "UPDATE FROM " + Person.class.getName() + " SET age = divide(divide(age, 2), 3) WHERE age = 90");
      int count = update.executeStatement();
      assertEquals(1, count);

      assertEquals(15, ((Person) cache(0).get("p1")).getAge());
      assertEquals(31, ((Person) cache(1).get("p2")).getAge());
   }

   /**
    * On a non-indexed distributed cache the matching keys are gathered with a
    * distributed stream and each entry is then computed from the node that
    * runs the statement, so the entries owned by the other node are updated by
    * a remotely marshalled {@code UpdateBiFunction}: the function is
    * deserialized and re-injected on the owner, which must re-parse the query
    * and resolve its own function registry.
    */
   @Test
   public void testDistributedUpdateWithExpressionOnNonIndexedCache() {
      String cacheName = "non-indexed-expression-update";
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(1);
      for (EmbeddedCacheManager cm : cacheManagers) {
         cm.defineConfiguration(cacheName, builder.build());
      }
      Cache<String, Person> cache0 = manager(0).getCache(cacheName);
      Cache<String, Person> cache1 = manager(1).getCache(cacheName);
      IntStream.range(0, 10).forEach(i ->
            (i % 2 == 0 ? cache0 : cache1).put("person" + i, new Person("name" + i, "blurb" + i, i)));

      // with one owner per key and this many keys, both nodes own some of them,
      // so the update of the other node's entries really crosses the wire
      long ownedByOtherNode = IntStream.range(0, 10)
            .filter(i -> !cache0.getAdvancedCache().getDistributionManager().getCacheTopology()
                  .getDistribution("person" + i).isPrimary())
            .count();
      assertTrue(ownedByOtherNode > 0,
            "expected some entries to be owned by the other node, but all 10 are local to node 0");

      Query<Person> update = cache0.query(
            "UPDATE FROM " + Person.class.getName() + " SET age = age + 100 WHERE age >= 0");
      assertEquals(10, update.executeStatement());

      for (int i = 0; i < 10; i++) {
         assertEquals(i + 100, ((Person) (i % 2 == 0 ? cache0 : cache1).get("person" + i)).getAge());
      }
   }
}
