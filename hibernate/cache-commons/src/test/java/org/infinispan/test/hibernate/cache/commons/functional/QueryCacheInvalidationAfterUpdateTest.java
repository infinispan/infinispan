package org.infinispan.test.hibernate.cache.commons.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.hibernate.stat.Statistics;
import org.hibernate.testing.orm.junit.JiraKey;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Person;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.junit.Test;

/**
 * Reproducer for https://github.com/infinispan/infinispan/issues/18037
 * <p>
 * After a transactional entity update, Hibernate calls:
 * 1. {@code TimestampsCache#preInvalidate} with timestamp {@code now + regionFactory.getTimeout()} (60 seconds)
 * 2. {@code TimestampsCache#invalidate} with timestamp {@code now}
 * <p>
 * In {@link org.infinispan.hibernate.cache.v6.impl.ClusteredTimestampsRegionImpl}, the local cache
 * is updated using {@code Math.max}, so the smaller "invalidate" timestamp never overwrites the larger
 * "preInvalidate" future timestamp. All subsequent query-cache reads return the future timestamp,
 * making every query appear stale for 60 seconds.
 */
@JiraKey(value = "ISPN-18037")
public class QueryCacheInvalidationAfterUpdateTest extends SingleNodeTest {

   protected static final ControlledTimeService TIME_SERVICE = new ControlledTimeService();

   @Override
   public List<Object[]> getParameters() {
      // Only clustered modes trigger ClusteredTimestampsRegionImpl which has the Math.max bug.
      return Arrays.asList(READ_WRITE_REPLICATED, READ_WRITE_DISTRIBUTED);
   }

   @Override
   protected Class[] getAnnotatedClasses() {
      return new Class[]{Person.class};
   }

   @Override
   protected void addSettings(Map settings) {
      super.addSettings(settings);
      settings.put(TestRegionFactory.TIME_SERVICE, TIME_SERVICE);
   }

   /**
    * Verifies that after an entity update, a second query executed after the invalidation
    * completes results in a query-cache HIT (not a miss lasting 60 seconds).
    */
   @Test
   public void testQueryCacheRestoredAfterEntityUpdate() throws Exception {
      // 1. Persist initial data
      Person john = new Person("John", "Black", 26);
      withTxSession(s -> s.persist(john));

      // Advance past the pre-invalidation window so the initial insert's future timestamp expires.
      TIME_SERVICE.advance(60001);

      Statistics statistics = sessionFactory().getStatistics();
      statistics.clear();

      // 2. First query: expected cache miss (populates the cache)
      withSession(s -> {
         queryPersons(s, 1);
         assertEquals(1, statistics.getQueryCacheMissCount(), "Expected 1 query cache miss on first read");
         assertEquals(1, statistics.getQueryCachePutCount(), "Expected 1 query cache put after first read");
      });
      statistics.clear();

      // 3. Second query: expected cache HIT (cache should be warm)
      withSession(s -> {
         queryPersons(s, 1);
         assertEquals(1, statistics.getQueryCacheHitCount(), "Expected query cache hit before update");
      });
      statistics.clear();

      // 4. Update the entity (triggers preInvalidate with now+60s, then invalidate with now)
      withTxSession(s -> {
         Person p = s.get(Person.class, john.getName());
         p.setAge(27);
      });

      // Advance time by 1ms — enough for the cache put from step 5 to be considered valid.
      TIME_SERVICE.advance(1);

      // 5. Query after update: expected cache miss (timestamps invalidated the entry)
      withSession(s -> {
         queryPersons(s, 1);
         assertEquals(1, statistics.getQueryCacheMissCount(), "Expected 1 query cache miss after update");
      });
      statistics.clear();

      // 6. Query again: expected cache HIT.
      // BUG: With the Math.max bug in ClusteredTimestampsRegionImpl, the localCache still holds
      // the future timestamp from preInvalidate (now+60s), so this query is seen as stale and
      // results in another miss instead of a hit.
      withSession(s -> {
         queryPersons(s, 1);
         assertEquals(1, statistics.getQueryCacheHitCount(),
               "Expected query cache hit after update invalidation resolved — " +
               "if this fails the localCache still holds the preInvalidate future timestamp");
      });
   }

   private void queryPersons(Session s, int expectedSize) {
      Query<Person> query = s.createQuery("from Person", Person.class)
            .setCacheable(true);
      List<Person> result = query.list();
      assertEquals(expectedSize, result.size());
   }
}
