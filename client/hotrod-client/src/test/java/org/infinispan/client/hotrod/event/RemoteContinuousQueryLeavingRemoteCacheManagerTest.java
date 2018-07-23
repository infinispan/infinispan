package org.infinispan.client.hotrod.event;


import static org.infinispan.query.dsl.Expression.param;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
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
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.api.continuous.ContinuousQueryListener;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.query.remote.impl.filter.IckleContinuousQueryProtobufCacheEventFilterConverterFactory;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.KeyValuePair;
import org.testng.annotations.Test;

/**
 * Test remote continuous query with removing a cache manager
 *
 * @author anistor@redhat.com
 * @author wburns@redhat.com
 * @since 10.0
 */
@Test(groups = "functional", testName = "client.hotrod.event.RemoteContinuousQueryLeavingRemoteCacheManagerTest")
public class RemoteContinuousQueryLeavingRemoteCacheManagerTest extends MultiHotRodServersTest {

   private final int NUM_NODES = 1;

   private RemoteCache<String, User> remoteCache;

   private ControlledTimeService timeService = new ControlledTimeService();

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

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = client(0).getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", Util.getResourceAsString("/sample_bank_account/bank.proto", getClass().getClassLoader()));
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(client(0)));
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder cfgBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      cfgBuilder.indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      cfgBuilder.expiration().disableReaper();
      return cfgBuilder;
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      return super.createHotRodClientConfigurationBuilder(host, serverPort)
            .marshaller(new ProtoStreamMarshaller());
   }

   static class Listener implements ContinuousQueryListener<String, User> {
      final BlockingQueue<KeyValuePair<String, User>> joined = new LinkedBlockingQueue<>();
      final BlockingQueue<KeyValuePair<String, User>> updated = new LinkedBlockingQueue<>();
      final BlockingQueue<String> left = new LinkedBlockingQueue<>();

      @Override
      public void resultJoining(String key, User value) {
         joined.add(new KeyValuePair<>(key, value));
      }

      @Override
      public void resultUpdated(String key, User value) {
         updated.add(new KeyValuePair<>(key, value));
      }

      @Override
      public void resultLeaving(String key) {
         left.add(key);
      }
   }

   private Listener applyContinuousQuery(RemoteCache<String, User> cacheToUse) {
      QueryFactory qf = Search.getQueryFactory(cacheToUse);

      Query query = qf.from(UserPB.class)
            .having("age").lte(param("ageParam"))
            .build()
            .setParameter("ageParam", 32);

      ContinuousQuery<String, User> continuousQuery = Search.getContinuousQuery(cacheToUse);
      Listener listener = new Listener();
      continuousQuery.addContinuousQueryListener(query, listener);

      return listener;
   }

   public void testContinuousQueryRemoveRCM() throws IOException {

      // Create an additional remote cache manager that registers the same query
      RemoteCacheManager extraRemoteCacheManager = new InternalRemoteCacheManager(createHotRodClientConfigurationBuilder(server(0)).build());
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(extraRemoteCacheManager));
      RemoteCache<String, User> extraRemoteCache = extraRemoteCacheManager.getCache();

      User user1 = new UserPB();
      user1.setId(1);
      user1.setName("John");
      user1.setSurname("Doe");
      user1.setGender(User.Gender.MALE);
      user1.setAge(22);
      user1.setAccountIds(new HashSet<>(Arrays.asList(1, 2)));
      user1.setNotes("Lorem ipsum dolor sit amet");

      remoteCache.put("user" + user1.getId(), user1);

      Listener listener = applyContinuousQuery(remoteCache);

      // Also register the query on the extra remote cache
      Listener extraListener = applyContinuousQuery(extraRemoteCache);

      expectElementsInQueue(listener.joined, 1, (kv) -> kv.getValue().getAge(), 22);
      expectElementsInQueue(extraListener.joined, 1, (kv) -> kv.getValue().getAge(), 22);
      expectElementsInQueue(listener.updated, 0);
      expectElementsInQueue(extraListener.updated, 0);
      expectElementsInQueue(listener.left, 0);
      expectElementsInQueue(extraListener.left, 0);

      // Now we shut down the extra remote cache
      extraRemoteCacheManager.stop();

      user1.setAge(23);
      remoteCache.put("user" + user1.getId(), user1);

      expectElementsInQueue(listener.joined, 0);
      expectElementsInQueue(listener.updated, 1, (kv) -> kv.getValue().getAge(), 23);
      expectElementsInQueue(listener.left, 0);
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

      try {
         // no more elements expected here
         Object o = queue.poll(100, TimeUnit.MILLISECONDS);
         assertNull("No more elements expected in queue!", o);
      } catch (InterruptedException e) {
         throw new AssertionError("Interrupted while waiting for condition", e);
      }
   }
}
