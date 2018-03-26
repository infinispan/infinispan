package org.infinispan.test.hibernate.cache.commons.query;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Properties;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.criterion.Restrictions;
import org.hibernate.engine.spi.CacheImplementor;
import org.hibernate.jpa.AvailableSettings;
import org.hibernate.testing.AfterClassOnce;
import org.hibernate.testing.BeforeClassOnce;
import org.hibernate.testing.TestForIssue;
import org.hibernate.testing.junit4.CustomRunner;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactoryProvider;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Person;
import org.infinispan.util.ControlledTimeService;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(CustomRunner.class)
public class QueryStalenessTest {
   ControlledTimeService timeService = new ControlledTimeService();
   SessionFactory sf1, sf2;

   @BeforeClassOnce
   public void init() {
      TestResourceTracker.testStarted(getClass().getSimpleName());
      sf1 = createSessionFactory();
      sf2 = createSessionFactory();
   }

   @AfterClassOnce
   public void destroy() {
      sf1.close();
      sf2.close();
      TestResourceTracker.testFinished(getClass().getSimpleName());
   }

   public SessionFactory createSessionFactory() {
      Configuration configuration = new Configuration()
            .setProperty(Environment.USE_SECOND_LEVEL_CACHE, "true")
            .setProperty(Environment.USE_QUERY_CACHE, "true")
            .setProperty(Environment.CACHE_REGION_FACTORY, TestRegionFactoryProvider.load().getRegionFactoryClass().getName())
            .setProperty(Environment.DEFAULT_CACHE_CONCURRENCY_STRATEGY, "transactional")
            .setProperty("hibernate.allow_update_outside_transaction", "true") // only applies in 5.2
            .setProperty(AvailableSettings.SHARED_CACHE_MODE, "ALL")
            .setProperty(Environment.HBM2DDL_AUTO, "create-drop");
      Properties testProperties = new Properties();
      testProperties.put(TestRegionFactory.TIME_SERVICE, timeService);
      configuration.addProperties(testProperties);
      configuration.addAnnotatedClass(Person.class);
      return configuration.buildSessionFactory();
   }

   @Test
   @TestForIssue(jiraKey = "HHH-10677")
   public void testLocalQueryInvalidatedImmediatelly() {
      Session s1 = sf1.openSession();
      Person person = new Person("John", "Smith", 29);
      s1.persist(person);
      s1.flush();
      s1.close();

      InfinispanBaseRegion timestampsRegion = TestRegionFactoryProvider.load().findTimestampsRegion((CacheImplementor) sf1.getCache());
      DistributionInfo distribution = timestampsRegion.getCache().getDistributionManager().getCacheTopology().getDistribution(Person.class.getSimpleName());
      SessionFactory qsf = distribution.isPrimary() ? sf2 : sf1;

      // The initial insert invalidates the queries for 60s to the future
      timeService.advance(60001);

      Session s2 = qsf.openSession();
      List<Person> list1 = s2.createCriteria(Person.class).setCacheable(true).add(Restrictions.le("age", 29)).list();
      assertEquals(1, list1.size());
      s2.close();

      Session s3 = qsf.openSession();
      Person p2 = s3.load(Person.class, person.getName());
      p2.setAge(30);
      s3.persist(p2);
      s3.flush();
      List<Person> list2 = s3.createCriteria(Person.class).setCacheable(true).add(Restrictions.le("age", 29)).list();
      assertEquals(0, list2.size());
      s3.close();
   }
}
