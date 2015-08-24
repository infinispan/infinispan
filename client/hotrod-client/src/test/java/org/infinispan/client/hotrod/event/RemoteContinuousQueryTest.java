package org.infinispan.client.hotrod.event;


import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.query.remote.impl.filter.JPAContinuousQueryCacheEventFilterConverterFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.Assert.*;


/**
 * @author anistor@redhat.com
 * @since 8.0
 */
@Test(groups = "functional", testName = "client.hotrod.event.RemoteContinuousQueryTest")
public class RemoteContinuousQueryTest extends MultiHotRodServersTest {

   private final int NUM_NODES = 5;

   private RemoteCache<Object, Object> remoteCache;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = getConfigurationBuilder();
      createHotRodServers(NUM_NODES, cfgBuilder);

      waitForClusterToForm();

      // Register the filter/converter factory. This should normally be discovered by the server via class path instead
      // of being added manually here, but this is ok in a test.
      JPAContinuousQueryCacheEventFilterConverterFactory factory = new JPAContinuousQueryCacheEventFilterConverterFactory();
      server(0).addCacheEventFilterConverterFactory(JPAContinuousQueryCacheEventFilterConverterFactory.FACTORY_NAME, factory);

      remoteCache = client(0).getCache();

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = client(0).getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", Util.read(Util.getResourceAsStream("/sample_bank_account/bank.proto", getClass().getClassLoader())));
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(client(0)));
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder cfgBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      cfgBuilder.indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return cfgBuilder;
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(int serverPort) {
      return super.createHotRodClientConfigurationBuilder(serverPort)
            .marshaller(new ProtoStreamMarshaller());
   }

   public void testContinuousQuery() throws Exception {
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
            .having("age").lte(32)
            .toBuilder().select("age").build();

      final BlockingQueue<Object> joined = new ArrayBlockingQueue<Object>(50);
      final BlockingQueue<Object> left = new ArrayBlockingQueue<Object>(50);

      ContinuousQueryListener listener = new ContinuousQueryListener() {

         @Override
         public void resultJoining(Object key, Object value) {
            joined.add(key);
         }

         @Override
         public void resultLeaving(Object key) {
            left.add(key);
         }
      };

      Object clientListener = ClientEvents.addContinuousQueryListener(remoteCache, listener, query);

      assertNotNull(clientListener);
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

      remoteCache.removeClientListener(clientListener);

      user2.setAge(22);
      remoteCache.put("user" + user2.getId(), user2);

      expectElementsInQueue(joined, 0);
      expectElementsInQueue(left, 0);
   }

   private void expectElementsInQueue(BlockingQueue<Object> queue, int numElements) {
      for (int i = 0; i < numElements; i++) {
         try {
            Object e = queue.poll(5, TimeUnit.SECONDS);
            assertNotNull(e);
         } catch (InterruptedException e) {
            throw new AssertionError("Interrupted while waiting for condition", e);
         }
      }
      try {
         // no more elements expected here
         Object e = queue.poll(5, TimeUnit.SECONDS);
         assertNull(e);
      } catch (InterruptedException e) {
         throw new AssertionError("Interrupted while waiting for condition", e);
      }
   }
}
