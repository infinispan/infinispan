package org.infinispan.query.continuous;

import static org.infinispan.query.dsl.Expression.max;
import static org.infinispan.query.dsl.Expression.param;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.query.Search;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
@Test(groups = "functional", testName = "query.continuous.ContinuousQueryTest")
public class ContinuousQueryTest extends SingleCacheManagerTest {

   protected ControlledTimeService timeService = new ControlledTimeService();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cacheConfiguration = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      //cacheConfiguration.transaction().lockingMode(LockingMode.PESSIMISTIC);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(cacheConfiguration);
      TestingUtil.replaceComponent(cm, TimeService.class, timeService, true);
      return cm;
   }

   /**
    * Fulltext continuous queries are not allowed.
    */
   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = ".*ISPN028521:.*")
   public void testDisallowFullTextQuery() {
      Query query = Search.getQueryFactory(cache()).create("from org.infinispan.query.test.Person where name : 'john'");

      ContinuousQuery<Object, Object> cq = Search.getContinuousQuery(cache());
      cq.addContinuousQueryListener(query, new CallCountingCQResultListener<>());
   }

   /**
    * Using grouping and aggregation with continuous query is not allowed.
    */
   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = ".*ISPN028509:.*")
   public void testDisallowGroupingAndAggregation() {
      Query query = Search.getQueryFactory(cache()).from(Person.class)
            .select(max("age"))
            .having("age").gte(20)
            .build();

      ContinuousQuery<Object, Object> cq = Search.getContinuousQuery(cache());
      cq.addContinuousQueryListener(query, new CallCountingCQResultListener<>());
   }

   public void testContinuousQuery() {
      for (int i = 0; i < 2; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(30 + i);
         cache().put(i, value);
      }

      QueryFactory qf = Search.getQueryFactory(cache());

      ContinuousQuery<Object, Object> cq = Search.getContinuousQuery(cache());

      Query query = qf.from(Person.class)
            .select("age")
            .having("age").lte(param("ageParam"))
            .build().setParameter("ageParam", 30);

      CallCountingCQResultListener<Object, Object> listener = new CallCountingCQResultListener<>();
      cq.addContinuousQueryListener(query, listener);

      final Map<Object, Integer> joined = listener.getJoined();
      final Map<Object, Integer> updated = listener.getUpdated();
      final Map<Object, Integer> left = listener.getLeft();

      assertEquals(1, joined.size());
      assertEquals(0, updated.size());
      assertEquals(0, left.size());
      joined.clear();

      for (int i = 0; i < 10; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);
         cache().put(i, value);
      }

      assertEquals(5, joined.size());
      assertEquals(1, updated.size());
      assertEquals(0, left.size());
      joined.clear();

      for (int i = 0; i < 2; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 40);
         cache().put(i, value);
      }

      assertEquals(0, joined.size());
      assertEquals(1, updated.size());
      assertEquals(2, left.size());
      left.clear();

      for (int i = 4; i < 20; i++) {
         cache().remove(i);
      }

      assertEquals(0, joined.size());
      assertEquals(1, updated.size());
      assertEquals(2, left.size());
      left.clear();

      cache().clear(); //todo [anistor] Does this generate MODIFY instead of REMOVE ???

      assertEquals(0, joined.size());
      assertEquals(1, updated.size());
      assertEquals(2, left.size());
      left.clear();

      for (int i = 0; i < 2; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 20);
         cache().put(i, value, 5, TimeUnit.MILLISECONDS);
      }

      assertEquals(2, joined.size());
      assertEquals(1, updated.size());
      assertEquals(0, left.size());
      joined.clear();

      timeService.advance(6);
      cache.getAdvancedCache().getExpirationManager().processExpiration();
      assertEquals(0, cache().size());

      assertEquals(0, joined.size());
      assertEquals(1, updated.size());
      assertEquals(2, left.size());
      left.clear();

      cq.removeContinuousQueryListener(listener);

      for (int i = 0; i < 3; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 20);
         cache().put(i, value);
      }

      assertEquals(0, joined.size());
      assertEquals(1, updated.size());
      assertEquals(0, left.size());
   }

   public void testContinuousQueryChangingParameter() {
      for (int i = 0; i < 2; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(30 + i);
         cache().put(i, value);
      }

      QueryFactory qf = Search.getQueryFactory(cache());

      ContinuousQuery<Object, Object> cq = Search.getContinuousQuery(cache());

      Query query = qf.from(Person.class)
            .select("age")
            .having("age").lte(param("ageParam"))
            .build();

      query.setParameter("ageParam", 30);

      CallCountingCQResultListener<Object, Object> listener = new CallCountingCQResultListener<>();
      cq.addContinuousQueryListener(query, listener);

      Map<Object, Integer> joined = listener.getJoined();
      Map<Object, Integer> updated = listener.getUpdated();
      Map<Object, Integer> left = listener.getLeft();

      assertEquals(1, joined.size());
      assertEquals(0, updated.size());
      assertEquals(0, left.size());
      joined.clear();

      cq.removeContinuousQueryListener(listener);

      query.setParameter("ageParam", 32);

      listener = new CallCountingCQResultListener<>();
      cq.addContinuousQueryListener(query, listener);

      joined = listener.getJoined();
      left = listener.getLeft();

      assertEquals(2, joined.size());
      assertEquals(0, updated.size());
      assertEquals(0, left.size());

      cq.removeContinuousQueryListener(listener);
   }

   public void testTwoSimilarCQ() {
      QueryFactory qf = Search.getQueryFactory(cache());
      CallCountingCQResultListener<Object, Object> listener = new CallCountingCQResultListener<>();

      Query query1 = qf.from(Person.class)
            .having("age").lte(30)
            .and().having("name").eq("John").or().having("name").eq("Johny")
            .build();
      ContinuousQuery<Object, Object> cq1 = Search.getContinuousQuery(cache());
      cq1.addContinuousQueryListener(query1, listener);

      Query query2 = qf.from(Person.class)
            .having("age").lte(30).or().having("name").eq("Joe")
            .build();
      ContinuousQuery<Object, Object> cq2 = Search.getContinuousQuery(cache());
      cq2.addContinuousQueryListener(query2, listener);

      final Map<Object, Integer> joined = listener.getJoined();
      final Map<Object, Integer> updated = listener.getUpdated();
      final Map<Object, Integer> left = listener.getLeft();

      assertEquals(0, joined.size());
      assertEquals(0, updated.size());
      assertEquals(0, left.size());

      Person value = new Person();
      value.setName("John");
      value.setAge(20);
      cache().put(1, value);

      assertEquals(1, joined.size());
      assertEquals(2, joined.get(1).intValue());
      assertEquals(0, updated.size());
      assertEquals(0, left.size());
      joined.clear();

      value = new Person();
      value.setName("Joe");
      cache().replace(1, value);
      assertEquals(0, joined.size());
      assertEquals(1, updated.size());
      assertEquals(1, left.size());
      joined.clear();
      left.clear();

      value = new Person();
      value.setName("Joe");
      value.setAge(31);
      cache().replace(1, value);
      assertEquals(0, joined.size());
      assertEquals(1, updated.size());
      assertEquals(0, left.size());
      joined.clear();
      left.clear();

      value = new Person();
      value.setName("John");
      value.setAge(29);
      cache().put(1, value);
      assertEquals(1, joined.size());
      assertEquals(1, joined.get(1).intValue());
      assertEquals(1, updated.size());
      assertEquals(0, left.size());
      joined.clear();
      left.clear();

      value = new Person();
      value.setName("Johny");
      value.setAge(29);
      cache().put(1, value);
      assertEquals(0, joined.size());
      assertEquals(1, updated.size());
      assertEquals(0, left.size());
      joined.clear();
      left.clear();

      cache().clear();
      assertEquals(0, joined.size());
      assertEquals(1, left.size());
      assertEquals(2, left.get(1).intValue());
      assertEquals(1, updated.size());
   }
}
