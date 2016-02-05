package org.infinispan.query.continuous;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hibernate.hql.ParsingException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Expression;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TimeService;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
@Test(groups = "functional", testName = "query.continuous.ContinuousQueryTest")
public class ContinuousQueryTest extends SingleCacheManagerTest {

   protected ControlledTimeService timeService = new ControlledTimeService(0);

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cacheConfiguration = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      //cacheConfiguration.transaction().lockingMode(LockingMode.PESSIMISTIC);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(cacheConfiguration);
      TestingUtil.replaceComponent(cm, TimeService.class, timeService, true);
      return cm;
   }

   /**
    * Using grouping and aggregation with continuous query is not allowed.
    */
   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = ".*ISPN000411:.*")
   public void testDisallowGroupingAndAggregation() {
      Query query = Search.getQueryFactory(cache()).from(Person.class)
            .select(Expression.max("age"))
            .having("age").gte(20)
            .toBuilder().build();

      ContinuousQuery<Object, Object> cq = new ContinuousQuery<>(cache());
      cq.addContinuousQueryListener(query, new CallCountingCQResultListener<>());
   }

   public void testContinuousQuery() {
      for (int i = 0; i < 2; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(30 + i);
         cache().put(i, value);
      }

      QueryFactory<?> qf = Search.getQueryFactory(cache());

      ContinuousQuery<Object, Object> cq = new ContinuousQuery<Object, Object>(cache());

      Query query = qf.from(Person.class)
            .select("age")
            .having("age").lte(Expression.param("ageParam"))
            .toBuilder().build().setParameter("ageParam", 30);

      CallCountingCQResultListener<Object, Object> listener = new CallCountingCQResultListener<>();
      cq.addContinuousQueryListener(query, listener);

      final Map<Object, Integer> joined = listener.getJoined();
      final Map<Object, Integer> left = listener.getLeft();

      assertEquals(1, joined.size());
      assertEquals(0, left.size());
      joined.clear();

      for (int i = 0; i < 10; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);
         cache().put(i, value);
      }

      assertEquals(5, joined.size());
      assertEquals(0, left.size());
      joined.clear();

      for (int i = 0; i < 2; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 40);
         cache().put(i, value);
      }

      assertEquals(0, joined.size());
      assertEquals(2, left.size());
      left.clear();

      for (int i = 4; i < 20; i++) {
         cache().remove(i);
      }

      assertEquals(0, joined.size());
      assertEquals(2, left.size());
      left.clear();

      cache().clear(); //todo [anistor] Does this generate MODIFY instead of REMOVE ???

      assertEquals(0, joined.size());
      assertEquals(2, left.size());
      left.clear();

      for (int i = 0; i < 2; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 20);
         cache().put(i, value, 5, TimeUnit.MILLISECONDS);
      }

      assertEquals(2, joined.size());
      assertEquals(0, left.size());
      joined.clear();

      timeService.advance(6);
      cache.getAdvancedCache().getExpirationManager().processExpiration();
      assertEquals(0, cache().size());

      assertEquals(0, joined.size());
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
      assertEquals(0, left.size());
   }

   public void testContinuousQueryChangingParameter() {
      for (int i = 0; i < 2; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(30 + i);
         cache().put(i, value);
      }

      QueryFactory<?> qf = Search.getQueryFactory(cache());

      ContinuousQuery<Object, Object> cq = new ContinuousQuery<Object, Object>(cache());

      Query query = qf.from(Person.class)
            .select("age")
            .having("age").lte(Expression.param("ageParam"))
            .toBuilder().build();

      query.setParameter("ageParam", 30);

      CallCountingCQResultListener<Object, Object> listener = new CallCountingCQResultListener<>();
      cq.addContinuousQueryListener(query, listener);

      Map<Object, Integer> joined = listener.getJoined();
      Map<Object, Integer> left = listener.getLeft();

      assertEquals(1, joined.size());
      assertEquals(0, left.size());
      joined.clear();

      cq.removeContinuousQueryListener(listener);

      query.setParameter("ageParam", 32);

      listener = new CallCountingCQResultListener<>();
      cq.addContinuousQueryListener(query, listener);

      joined = listener.getJoined();
      left = listener.getLeft();

      assertEquals(2, joined.size());
      assertEquals(0, left.size());
   }

   public void testTwoSimilarCQ() {
      QueryFactory<?> qf = Search.getQueryFactory(cache());
      CallCountingCQResultListener<Object, Object> listener = new CallCountingCQResultListener<>();

      Query query1 = qf.from(Person.class)
            .having("age").lte(30)
            .and().having("name").eq("John").or().having("name").eq("Johny")
            .toBuilder().build();
      ContinuousQuery<Object, Object> cq1 = new ContinuousQuery<Object, Object>(cache());
      cq1.addContinuousQueryListener(query1, listener);

      Query query2 = qf.from(Person.class)
            .having("age").lte(30).or().having("name").eq("Joe")
            .toBuilder().build();
      ContinuousQuery<Object, Object> cq2 = new ContinuousQuery<Object, Object>(cache());
      cq2.addContinuousQueryListener(query2, listener);

      final Map<Object, Integer> joined = listener.getJoined();
      final Map<Object, Integer> left = listener.getLeft();

      assertEquals(0, joined.size());
      assertEquals(0, left.size());

      Person value = new Person();
      value.setName("John");
      value.setAge(20);
      cache().put(1, value);

      assertEquals(1, joined.size());
      assertEquals(2, joined.get(1).intValue());
      assertEquals(0, left.size());
      joined.clear();

      value = new Person();
      value.setName("Joe");
      cache().replace(1, value);
      assertEquals(0, joined.size());
      assertEquals(1, left.size());
      joined.clear();
      left.clear();

      value = new Person();
      value.setName("Joe");
      value.setAge(31);
      cache().replace(1, value);
      assertEquals(0, joined.size());
      assertEquals(0, left.size());
      joined.clear();
      left.clear();

      value = new Person();
      value.setName("John");
      value.setAge(29);
      cache().put(1, value);
      assertEquals(1, joined.size());
      assertEquals(1, joined.get(1).intValue());
      assertEquals(0, left.size());
      joined.clear();
      left.clear();

      value = new Person();
      value.setName("Johny");
      value.setAge(29);
      cache().put(1, value);
      assertEquals(0, joined.size());
      assertEquals(0, left.size());
      joined.clear();
      left.clear();

      cache().clear();
      assertEquals(0, joined.size());
      assertEquals(1, left.size());
      assertEquals(2, left.get(1).intValue());
   }

}
