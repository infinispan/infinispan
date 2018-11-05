package org.infinispan.test.hibernate.cache.commons.functional;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.stat.Statistics;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Person;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.util.ControlledTimeService;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class NonTxQueryTest extends SingleNodeTest {

   protected static final ControlledTimeService TIME_SERVICE = new ControlledTimeService();

   @Override
   public List<Object[]> getParameters() {
      return getParameters(true, true, true, true);
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

   @Test
   public void testNonTransactionalQuery() throws Exception {
      Person john = new Person("John", "Black", 26);
      Person peter = new Person("Peter", "White", 32);

      withTxSession(s -> {
         s.persist(john);
         s.persist(peter);
      });

      // Delay added to guarantee that query cache results won't be considered
      // as not up to date due to persist session and query results from first
      // query happening simultaneously.
      TIME_SERVICE.advance(60001);

      Statistics statistics = sessionFactory().getStatistics();
      statistics.clear();

      withSession(s -> {
         queryPersons(s);
         assertEquals(1, statistics.getQueryCacheMissCount());
         assertEquals(1, statistics.getQueryCachePutCount());
      });

      statistics.clear();

      withSession(s -> {
         queryPersons(s);
         // assertEquals(2, statistics.getSecondLevelCacheHitCount());
         assertEquals(1, statistics.getQueryCacheHitCount());
      });
   }

   public void queryPersons(Session s) {
      Query query = s
         .createQuery("from Person")
         .setCacheable(true);

      List<Person> result = (List<Person>) query.list();
      assertEquals(2, result.size());
   }

}
