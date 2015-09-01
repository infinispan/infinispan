package org.infinispan.query.continuous;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
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
   protected ExpirationManager manager;
   
   @Override
   protected void setup() throws Exception {
      super.setup();
      manager = TestingUtil.extractComponent(cache, ExpirationManager.class);
   }
   
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cacheConfiguration = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      //cacheConfiguration.transaction().lockingMode(LockingMode.PESSIMISTIC);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(cacheConfiguration);
      TestingUtil.replaceComponent(cm, TimeService.class, timeService, true);
      return cm;
   }

   public void testContinuousQuery() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache());

      ContinuousQuery<Object, Object> cq = new ContinuousQuery<Object, Object>(cache());

      Query query = qf.from(Person.class)
            .having("age").lte(30)
            .toBuilder().build();

      final Set<Object> joined = new HashSet<Object>();
      final Set<Object> left = new HashSet<Object>();

      ContinuousQueryResultListener<Object, Object> listener = new ContinuousQueryResultListener<Object, Object>() {
         @Override
         public void resultJoining(Object key, Object value) {
            joined.add(key);
         }

         @Override
         public void resultLeaving(Object key) {
            left.add(key);
         }
      };

      cq.addContinuousQueryListener(query, listener);

      assertEquals(0, joined.size());
      assertEquals(0, left.size());

      for (int i = 0; i < 2; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(40);
         cache().put(i, value);
      }

      assertEquals(0, joined.size());
      assertEquals(0, left.size());

      for (int i = 0; i < 10; i++) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);
         cache().put(i, value);
      }

      assertEquals(6, joined.size());
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
      manager.processExpiration();
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
}
