package org.infinispan.client.hotrod.event;


import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.marshall.EmbeddedUserMarshaller;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.GenderMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.api.continuous.ContinuousQueryListener;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.remote.CompatibilityProtoStreamMarshaller;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.query.remote.impl.filter.JPAContinuousQueryProtobufCacheEventFilterConverterFactory;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.infinispan.query.dsl.Expression.max;
import static org.infinispan.query.dsl.Expression.param;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


/**
 * Test remote continuous query in compat mode.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
@Test(groups = "functional", testName = "client.hotrod.event.EmbeddedCompatContinuousQueryTest")
public class EmbeddedCompatContinuousQueryTest extends MultiHotRodServersTest {

   private final int NUM_NODES = 5;

   private RemoteCache<String, User> remoteCache;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = getConfigurationBuilder();
      createHotRodServers(NUM_NODES, cfgBuilder);

      waitForClusterToForm();

      // Register the filter/converter factory. This should normally be discovered by the server via class path instead
      // of being added manually here, but this is ok in a test.
      JPAContinuousQueryProtobufCacheEventFilterConverterFactory factory = new JPAContinuousQueryProtobufCacheEventFilterConverterFactory();
      server(0).addCacheEventFilterConverterFactory(JPAContinuousQueryProtobufCacheEventFilterConverterFactory.FACTORY_NAME, factory);

      remoteCache = client(0).getCache();

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = client(0).getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", Util.read(Util.getResourceAsStream("/sample_bank_account/bank.proto", getClass().getClassLoader())));
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      for (int i = 0; i < NUM_NODES; i++) {
         ProtobufMetadataManager protobufMetadataManager = manager(i).getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
         protobufMetadataManager.registerMarshaller(new EmbeddedUserMarshaller());
         protobufMetadataManager.registerMarshaller(new GenderMarshaller());
      }

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(client(0)));
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder cfgBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      cfgBuilder.dataContainer().keyEquivalence(AnyEquivalence.getInstance());
      cfgBuilder.compatibility().enable().marshaller(new CompatibilityProtoStreamMarshaller());
      cfgBuilder.indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      cfgBuilder.expiration().disableReaper();
      return cfgBuilder;
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(int serverPort) {
      return super.createHotRodClientConfigurationBuilder(serverPort)
            .marshaller(new ProtoStreamMarshaller());
   }

   /**
    * Using grouping and aggregation with continuous query is not allowed.
    */
   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN000411:.*")
   public void testDisallowGroupingAndAggregation() {
      Query query = Search.getQueryFactory(remoteCache).from(UserPB.class)
            .select(max("age"))
            .having("age").gte(20)
            .toBuilder().build();

      ContinuousQuery<String, User> continuousQuery = Search.getContinuousQuery(remoteCache);

      ContinuousQueryListener<String, Object[]> listener = new ContinuousQueryListener<String, Object[]>() {
         @Override
         public void resultJoining(String key, Object[] value) {
         }

         @Override
         public void resultLeaving(String key) {
         }
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
      user1.setAccountIds(new HashSet<Integer>(Arrays.asList(1, 2)));
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
      user3.setAccountIds(Collections.<Integer>emptySet());

      remoteCache.put("user" + user1.getId(), user1);
      remoteCache.put("user" + user2.getId(), user2);
      remoteCache.put("user" + user3.getId(), user3);
      assertEquals(3, remoteCache.size());

      QueryFactory qf = Search.getQueryFactory(remoteCache);

      Query query = qf.from(UserPB.class)
            .select("age")
            .having("age").lte(param("ageParam"))
            .toBuilder().build()
            .setParameter("ageParam", 32);

      final BlockingQueue<String> joined = new LinkedBlockingQueue<String>();
      final BlockingQueue<String> left = new LinkedBlockingQueue<String>();

      ContinuousQueryListener<String, Object[]> listener = new ContinuousQueryListener<String, Object[]>() {

         @Override
         public void resultJoining(String key, Object[] value) {
            joined.add(key);
         }

         @Override
         public void resultLeaving(String key) {
            left.add(key);
         }
      };

      ContinuousQuery<String, User> continuousQuery = Search.getContinuousQuery(remoteCache);
      continuousQuery.addContinuousQueryListener(query, listener);

      expectElementsInQueue(joined, 2);
      expectElementsInQueue(left, 0);

      user3.setAge(30);
      remoteCache.put("user" + user3.getId(), user3);

      expectElementsInQueue(joined, 1);
      expectElementsInQueue(left, 0);

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

      TestingUtil.sleepThread(60);
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

   public void testContinuousQueryChangingParameter() {
      User user1 = new UserPB();
      user1.setId(1);
      user1.setName("John");
      user1.setSurname("Doe");
      user1.setGender(User.Gender.MALE);
      user1.setAge(22);
      user1.setAccountIds(new HashSet<Integer>(Arrays.asList(1, 2)));
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
      user3.setAccountIds(Collections.<Integer>emptySet());

      remoteCache.put("user" + user1.getId(), user1);
      remoteCache.put("user" + user2.getId(), user2);
      remoteCache.put("user" + user3.getId(), user3);
      assertEquals(3, remoteCache.size());

      QueryFactory qf = Search.getQueryFactory(remoteCache);

      Query query = qf.from(UserPB.class)
            .select("age")
            .having("age").lte(param("ageParam"))
            .toBuilder().build()
            .setParameter("ageParam", 32);

      final BlockingQueue<String> joined = new LinkedBlockingQueue<String>();
      final BlockingQueue<String> left = new LinkedBlockingQueue<String>();

      ContinuousQueryListener<String, Object[]> listener = new ContinuousQueryListener<String, Object[]>() {

         @Override
         public void resultJoining(String key, Object[] value) {
            joined.add(key);
         }

         @Override
         public void resultLeaving(String key) {
            left.add(key);
         }
      };

      ContinuousQuery<String, User> cq = Search.getContinuousQuery(remoteCache);
      cq.addContinuousQueryListener(query, listener);

      expectElementsInQueue(joined, 2);
      expectElementsInQueue(left, 0);

      joined.clear();
      left.clear();

      cq.removeContinuousQueryListener(listener);

      query.setParameter("ageParam", 40);

      listener = new ContinuousQueryListener<String, Object[]>() {

         @Override
         public void resultJoining(String key, Object[] value) {
            joined.add(key);
         }

         @Override
         public void resultLeaving(String key) {
            left.add(key);
         }
      };

      cq.addContinuousQueryListener(query, listener);

      expectElementsInQueue(joined, 3);
      expectElementsInQueue(left, 0);
   }

   private void expectElementsInQueue(BlockingQueue<?> queue, int numElements) {
      for (int i = 0; i < numElements; i++) {
         try {
            Object e = queue.poll(5, TimeUnit.SECONDS);
            assertNotNull("Queue was empty!", e);
         } catch (InterruptedException e) {
            throw new AssertionError("Interrupted while waiting for condition", e);
         }
      }
      try {
         // no more elements expected here
         Object e = queue.poll(5, TimeUnit.SECONDS);
         assertNull("No more elements expected in queue!", e);
      } catch (InterruptedException e) {
         throw new AssertionError("Interrupted while waiting for condition", e);
      }
   }
}
