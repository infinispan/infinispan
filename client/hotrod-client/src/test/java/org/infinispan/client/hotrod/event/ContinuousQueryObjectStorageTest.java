package org.infinispan.client.hotrod.event;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.TestDomainSCI;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.api.query.ContinuousQuery;
import org.infinispan.commons.api.query.ContinuousQueryListener;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS;
import org.infinispan.query.remote.impl.GlobalContextInitializer;
import org.infinispan.query.remote.impl.filter.IckleContinuousQueryProtobufCacheEventFilterConverterFactory;
import org.infinispan.test.TestingUtil;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.util.KeyValuePair;
import org.testng.annotations.Test;

/**
 * Test remote continuous query when storing objects.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
@Test(groups = "functional", testName = "client.hotrod.event.ContinuousQueryObjectStorageTest")
public class ContinuousQueryObjectStorageTest extends MultiHotRodServersTest {

   private static final int NUM_NODES = 5;

   private RemoteCache<String, User> remoteCache;

   private final ControlledTimeService timeService = new ControlledTimeService();

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = getConfigurationBuilder();
      createHotRodServers(NUM_NODES, cfgBuilder);

      waitForClusterToForm();

      // Register the filter/converter factory. This should normally be discovered by the server via class path instead
      // of being added manually here, but this is ok in a test.
      IckleContinuousQueryProtobufCacheEventFilterConverterFactory factory = new IckleContinuousQueryProtobufCacheEventFilterConverterFactory();
      for (int i = 0; i < NUM_NODES; i++) {
         server(i).addCacheEventFilterConverterFactory(IckleContinuousQueryProtobufCacheEventFilterConverterFactory.FACTORY_NAME, factory);
         TestingUtil.replaceComponent(server(i).getCacheManager(), TimeService.class, timeService, true);
      }
      remoteCache = client(0).getCache();
   }

   @Override
   protected List<SerializationContextInitializer> contextInitializers() {
      return Arrays.asList(GlobalContextInitializer.INSTANCE, TestDomainSCI.INSTANCE, ClientEventSCI.INSTANCE);
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder cfgBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      cfgBuilder.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      cfgBuilder.encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      cfgBuilder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntities(UserHS.class);
      cfgBuilder.expiration().disableReaper();
      return cfgBuilder;
   }

   /**
    * Using grouping and aggregation with continuous query is not allowed.
    */
   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN028509:.*")
   public void testDisallowGroupingAndAggregation() {
      Query<Object[]> query = remoteCache.query("SELECT MAX(age) FROM sample_bank_account.User WHERE age >= 20");
      ContinuousQuery<String, User> continuousQuery = remoteCache.continuousQuery();

      ContinuousQueryListener<String, Object[]> listener = new ContinuousQueryListener<String, Object[]>() {
      };
      continuousQuery.addContinuousQueryListener(query, listener);
   }

   public void testContinuousQuery() {
      User user1 = new UserPB();
      user1.setId(1);
      user1.setName("John");
      user1.setSurname("Doe");
      user1.setGender(User.Gender.MALE);
      user1.setAge(22);
      user1.setAccountIds(new HashSet<>(Arrays.asList(1, 2)));
      user1.setNotes("Lorem ipsum dolor sit amet");

      User user2 = new UserPB();
      user2.setId(2);
      user2.setName("Spider");
      user2.setSurname("Man");
      user2.setGender(User.Gender.MALE);
      user2.setAge(32);
      user2.setAccountIds(Collections.singleton(3));

      User user3 = new UserPB();
      user3.setId(3);
      user3.setName("Spider");
      user3.setSurname("Woman");
      user3.setGender(User.Gender.FEMALE);
      user3.setAge(40);

      remoteCache.clear();
      remoteCache.put("user" + user1.getId(), user1);
      remoteCache.put("user" + user2.getId(), user2);
      remoteCache.put("user" + user3.getId(), user3);
      assertEquals(3, remoteCache.size());

      Query<User> query = remoteCache.<User>query("FROM sample_bank_account.User WHERE age <= :ageParam")
            .setParameter("ageParam", 32);

      final BlockingQueue<KeyValuePair<String, User>> joined = new LinkedBlockingQueue<>();
      final BlockingQueue<String> left = new LinkedBlockingQueue<>();

      ContinuousQueryListener<String, User> listener = new ContinuousQueryListener<String, User>() {

         @Override
         public void resultJoining(String key, User value) {
            joined.add(new KeyValuePair<>(key, value));
         }

         @Override
         public void resultLeaving(String key) {
            left.add(key);
         }
      };

      ContinuousQuery<String, User> continuousQuery = remoteCache.continuousQuery();
      continuousQuery.addContinuousQueryListener(query, listener);

      expectElementsInQueue(joined, 2, (kv) -> kv.getValue().getAge(), 32, 22);
      expectElementsInQueue(left, 0);
      expectNoMoreElementsInQueues(joined, left);

      user3.setAge(30);
      remoteCache.put("user" + user3.getId(), user3);

      expectElementsInQueue(joined, 1, (kv) -> kv.getValue().getAge(), 30);
      expectElementsInQueue(left, 0);
      expectNoMoreElementsInQueues(joined, left);

      user1.setAge(40);
      user2.setAge(40);
      user3.setAge(40);
      remoteCache.put("user" + user1.getId(), user1);
      remoteCache.put("user" + user2.getId(), user2);
      remoteCache.put("user" + user3.getId(), user3);

      expectElementsInQueue(joined, 0);
      expectElementsInQueue(left, 3);
      expectNoMoreElementsInQueues(joined, left);

      remoteCache.clear();
      user1.setAge(21);
      user2.setAge(22);
      remoteCache.put("expiredUser1", user1, 5, TimeUnit.MILLISECONDS);
      remoteCache.put("expiredUser2", user2, 5, TimeUnit.MILLISECONDS);

      expectElementsInQueue(joined, 2);
      expectElementsInQueue(left, 0);
      expectNoMoreElementsInQueues(joined, left);

      timeService.advance(6);
      assertNull(remoteCache.get("expiredUser1"));
      assertNull(remoteCache.get("expiredUser2"));

      expectElementsInQueue(joined, 0);
      expectElementsInQueue(left, 2);
      expectNoMoreElementsInQueues(joined, left);

      continuousQuery.removeContinuousQueryListener(listener);

      user2.setAge(22);
      remoteCache.put("user" + user2.getId(), user2);

      expectElementsInQueue(joined, 0);
      expectElementsInQueue(left, 0);
      expectNoMoreElementsInQueues(joined, left);
   }

   public void testContinuousQueryWithProjections() throws InterruptedException {
      User user1 = new UserPB();
      user1.setId(1);
      user1.setName("John");
      user1.setSurname("Doe");
      user1.setGender(User.Gender.MALE);
      user1.setAge(22);
      user1.setAccountIds(new HashSet<>(Arrays.asList(1, 2)));
      user1.setNotes("Lorem ipsum dolor sit amet");

      User user2 = new UserPB();
      user2.setId(2);
      user2.setName("Spider");
      user2.setSurname("Man");
      user2.setGender(User.Gender.MALE);
      user2.setAge(32);
      user2.setAccountIds(Collections.singleton(3));

      User user3 = new UserPB();
      user3.setId(3);
      user3.setName("Spider");
      user3.setSurname("Woman");
      user3.setGender(User.Gender.FEMALE);
      user3.setAge(40);

      remoteCache.clear();
      remoteCache.put("user" + user1.getId(), user1);
      remoteCache.put("user" + user2.getId(), user2);
      remoteCache.put("user" + user3.getId(), user3);
      assertEquals(3, remoteCache.size());

      Query<Object[]> query = remoteCache.<Object[]>query("SELECT age FROM sample_bank_account.User WHERE age <= :ageParam")
                      .setParameter("ageParam", 32);

      final BlockingQueue<KeyValuePair<String, Object[]>> joined = new LinkedBlockingQueue<>();
      final BlockingQueue<String> left = new LinkedBlockingQueue<>();

      ContinuousQueryListener<String, Object[]> listener = new ContinuousQueryListener<String, Object[]>() {

         @Override
         public void resultJoining(String key, Object[] value) {
            joined.add(new KeyValuePair<>(key, value));
         }

         @Override
         public void resultLeaving(String key) {
            left.add(key);
         }
      };

      ContinuousQuery<String, User> continuousQuery = remoteCache.continuousQuery();
      continuousQuery.addContinuousQueryListener(query, listener);

      expectElementsInQueue(joined, 2, (kv) -> kv.getValue()[0], 32, 22);
      expectElementsInQueue(left, 0);
      expectNoMoreElementsInQueues(joined, left);

      user3.setAge(30);
      remoteCache.put("user" + user3.getId(), user3);

      expectElementsInQueue(joined, 1, (kv) -> kv.getValue()[0], 30);
      expectElementsInQueue(left, 0);
      expectNoMoreElementsInQueues(joined, left);

      user1.setAge(40);
      user2.setAge(40);
      user3.setAge(40);
      remoteCache.put("user" + user1.getId(), user1);
      remoteCache.put("user" + user2.getId(), user2);
      remoteCache.put("user" + user3.getId(), user3);

      expectElementsInQueue(joined, 0);
      expectElementsInQueue(left, 3);

      remoteCache.clear();
      user1.setAge(21);
      user2.setAge(22);
      remoteCache.put("expiredUser1", user1, 5, TimeUnit.MILLISECONDS);
      remoteCache.put("expiredUser2", user2, 5, TimeUnit.MILLISECONDS);

      expectElementsInQueue(joined, 2);
      expectElementsInQueue(left, 0);

      timeService.advance(6);
      assertNull(remoteCache.get("expiredUser1"));
      assertNull(remoteCache.get("expiredUser2"));

      expectElementsInQueue(joined, 0);
      expectElementsInQueue(left, 2);

      continuousQuery.removeContinuousQueryListener(listener);

      user2.setAge(22);
      remoteCache.put("user" + user2.getId(), user2);

      expectElementsInQueue(joined, 0);
      expectElementsInQueue(left, 0);
   }

   public void testContinuousQueryChangingParameter() throws InterruptedException {
      User user1 = new UserPB();
      user1.setId(1);
      user1.setName("John");
      user1.setSurname("Doe");
      user1.setGender(User.Gender.MALE);
      user1.setAge(22);
      user1.setAccountIds(new HashSet<>(Arrays.asList(1, 2)));
      user1.setNotes("Lorem ipsum dolor sit amet");

      User user2 = new UserPB();
      user2.setId(2);
      user2.setName("Spider");
      user2.setSurname("Man");
      user2.setGender(User.Gender.MALE);
      user2.setAge(32);
      user2.setAccountIds(Collections.singleton(3));

      User user3 = new UserPB();
      user3.setId(3);
      user3.setName("Spider");
      user3.setSurname("Woman");
      user3.setGender(User.Gender.FEMALE);
      user3.setAge(40);

      remoteCache.clear();
      remoteCache.put("user" + user1.getId(), user1);
      remoteCache.put("user" + user2.getId(), user2);
      remoteCache.put("user" + user3.getId(), user3);
      assertEquals(3, remoteCache.size());

      Query<Object[]> query = remoteCache.<Object[]>query("SELECT age FROM sample_bank_account.User WHERE age <= :ageParam")
                      .setParameter("ageParam", 32);

      final BlockingQueue<KeyValuePair<String, Object[]>> joined = new LinkedBlockingQueue<>();
      final BlockingQueue<String> left = new LinkedBlockingQueue<>();

      ContinuousQueryListener<String, Object[]> listener = new ContinuousQueryListener<String, Object[]>() {

         @Override
         public void resultJoining(String key, Object[] value) {
            joined.add(new KeyValuePair<>(key, value));
         }

         @Override
         public void resultLeaving(String key) {
            left.add(key);
         }
      };

      ContinuousQuery<String, User> cq = remoteCache.continuousQuery();
      cq.addContinuousQueryListener(query, listener);

      expectElementsInQueue(joined, 2, (kv) -> kv.getValue()[0], 32, 22);
      expectElementsInQueue(left, 0);
      expectNoMoreElementsInQueues(joined, left);

      joined.clear();
      left.clear();

      cq.removeContinuousQueryListener(listener);

      query.setParameter("ageParam", 40);

      listener = new ContinuousQueryListener<String, Object[]>() {

         @Override
         public void resultJoining(String key, Object[] value) {
            joined.add(new KeyValuePair<>(key, value));
         }

         @Override
         public void resultLeaving(String key) {
            left.add(key);
         }
      };

      cq.addContinuousQueryListener(query, listener);

      expectElementsInQueue(joined, 3);
      expectElementsInQueue(left, 0);

      cq.removeContinuousQueryListener(listener);
   }

   private <T> void expectElementsInQueue(BlockingQueue<T> queue, int numElements) {
      expectElementsInQueue(queue, numElements, null);
   }

   private <T, R> void expectElementsInQueue(BlockingQueue<T> queue, int numElements, Function<T, R> valueTransformer, Object... expectedValue) {
      List<Object> expectedValues;
      if (expectedValue.length != 0) {
         if (expectedValue.length != numElements) {
            throw new IllegalArgumentException("The number of expected values must either match the number of expected elements or no expected values should be specified.");
         }
         expectedValues = new ArrayList<>(expectedValue.length);
         Collections.addAll(expectedValues, expectedValue);
      } else {
         expectedValues = null;
      }

      for (int i = 0; i < numElements; i++) {
         final T o;
         try {
            o = queue.poll(5, TimeUnit.SECONDS);
            assertNotNull("Queue was empty after reading " + i + " elements!", o);
         } catch (InterruptedException e) {
            throw new AssertionError("Interrupted while waiting for condition", e);
         }
         if (expectedValues != null) {
            Object v = valueTransformer != null ? valueTransformer.apply(o) : o;
            boolean found = expectedValues.remove(v);
            assertTrue("Expectation failed on element number " + i + ", unexpected value: " + v, found);
         }
      }
   }

   private void expectNoMoreElementsInQueues(BlockingQueue<?>... queues) {
      // A short delay gives unexpected events in transit a chance to appear in the queue
      TestingUtil.sleepThread(100);

      for (BlockingQueue<?> queue : queues) {
         assertNull("No more elements expected in queue!", queue.poll());
      }
   }

}
