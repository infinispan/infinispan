package org.infinispan.query.blackbox;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
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
}
