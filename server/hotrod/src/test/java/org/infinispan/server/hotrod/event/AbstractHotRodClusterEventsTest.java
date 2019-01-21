package org.infinispan.server.hotrod.event;

import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.withClientListener;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.server.hotrod.HotRodMultiNodeTest;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.hotrod.test.TestClientListener;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.KeyValuePair;
import org.testng.annotations.Test;

/**
 * @author Galder Zamarreño
 */
@Test(groups = "functional")
public abstract class AbstractHotRodClusterEventsTest extends HotRodMultiNodeTest {

   private ArrayList<AcceptedKeyFilterFactory> filters = new ArrayList<>();
   private ArrayList<AcceptedKeyValueConverterFactory> converters = new ArrayList<>();

   @Override
   protected String cacheName() {
      return "remote-clustered-events";
   }

   @Override
   protected int nodeCount() {
      return 3;
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(cacheMode, false));
   }

   @Override
   protected HotRodServer startTestHotRodServer(EmbeddedCacheManager cacheManager, int port) {
      HotRodServer server = HotRodTestingUtil.startHotRodServer(cacheManager, port);
      filters.add(new AcceptedKeyFilterFactory());
      server.addCacheEventFilterFactory("accepted-key-filter-factory", filters.get(0));
      converters.add(new AcceptedKeyValueConverterFactory());
      server.addCacheEventConverterFactory("accepted-keyvalue-converter-factory", converters.get(0));
      return server;
   }

   public void testEventForwarding(Method m) {
      // Registering listener in one node and executing operations against
      // different nodes should still result in events received
      HotRodClient client1 = clients().get(0);
      HotRodClient client2 = clients().get(1);
      HotRodClient client3 = clients().get(2);
      EventLogListener listener1 = new EventLogListener();
      withClientListener(client1, listener1, Optional.empty(), Optional.empty(), false, true, () -> {
         byte[] key = k(m);
         client2.put(key, 0, 0, v(m));
         listener1.expectOnlyCreatedEvent(anyCache(), key);
         client3.put(key, 0, 0, v(m, "v2-"));
         listener1.expectOnlyModifiedEvent(anyCache(), key);
         client2.remove(key);
         listener1.expectOnlyRemovedEvent(anyCache(), key);
      });
   }

   public void testNoEventsAfterRemovingListener(Method m) {
      HotRodClient client1 = clients().get(0);
      EventLogListener listener1 = new EventLogListener();
      byte[] key = k(m);
      withClientListener(client1, listener1, Optional.empty(), Optional.empty(), false, true, () -> {
         client1.put(key, 0, 0, v(m));
         listener1.expectOnlyCreatedEvent(anyCache(), key);
         client1.put(key, 0, 0, v(m, "v2-"));
         listener1.expectOnlyModifiedEvent(anyCache(), key);
         client1.remove(key);
         listener1.expectOnlyRemovedEvent(anyCache(), key);
      });
      client1.put(key, 0, 0, v(m));
      listener1.expectNoEvents(Optional.empty());
      client1.remove(key);
      listener1.expectNoEvents(Optional.empty());
   }

   public void testNoEventsAfterRemovingListenerInDifferentNode(Method m) {
      HotRodClient client1 = clients().get(0);
      HotRodClient client2 = clients().get(1);
      EventLogListener listener1 = new EventLogListener();
      byte[] key = k(m);
      assertStatus(client1.addClientListener(listener1, false, Optional.empty(), Optional.empty(), true), Success);
      try {
         client1.put(key, 0, 0, v(m));
         listener1.expectOnlyCreatedEvent(anyCache(), key);
         client1.put(key, 0, 0, v(m, "v2-"));
         listener1.expectOnlyModifiedEvent(anyCache(), key);
         client1.remove(key);
         listener1.expectOnlyRemovedEvent(anyCache(), key);
         // Use a client connected to a different node to attempt trying to remove listener
         client2.removeClientListener(listener1.getId());
         // The remoint has no effect since the listener information is not clustered
         // Remoint needs to be done in the node where the listener was added
         client1.put(key, 0, 0, v(m));
         listener1.expectOnlyCreatedEvent(anyCache(), key);
         client1.remove(key);
         listener1.expectOnlyRemovedEvent(anyCache(), key);
      } finally {
         assertStatus(client1.removeClientListener(listener1.getId()), Success);
      }
   }

   public void testClientDisconnectListenerCleanup(Method m) throws InterruptedException {
      HotRodClient client1 = clients().get(0);
      HotRodClient
            newClient = new HotRodClient("127.0.0.1", servers().get(1).getPort(), cacheName(), 60, protocolVersion());
      EventLogListener listener = new EventLogListener();
      assertStatus(newClient.addClientListener(listener, false, Optional.empty(), Optional.empty(), true), Success);
      byte[] key = k(m);
      client1.put(key, 0, 0, v(m));
      listener.expectOnlyCreatedEvent(anyCache(), key);
      newClient.stop().await();
      client1.put(k(m, "k2-"), 0, 0, v(m));
      listener.expectNoEvents(Optional.empty());
      client1.remove(key);
      client1.remove(k(m, "k2-"));
   }

   public void testFailoverSendsEventsForNewContent(Method m) {
      HotRodClient client1 = clients().get(0);
      HotRodClient client2 = clients().get(1);
      HotRodClient client3 = clients().get(2);
      EventLogListener listener1 = new EventLogListener();
      EventLogListener listener2 = new EventLogListener();
      withClientListener(client1, listener1, Optional.empty(), Optional.empty(), false, true, () -> {
         byte[] key = k(m);
         client2.put(key, 0, 0, v(m));
         listener1.expectOnlyCreatedEvent(anyCache(), key);
         client2.remove(key);
         listener1.expectOnlyRemovedEvent(anyCache(), key);
         HotRodServer newServer = startClusteredServer(servers().get(2).getPort() + 50);
         HotRodClient client4 = new HotRodClient("127.0.0.1", newServer.getPort(), cacheName(), 60, protocolVersion());
         try {
            withClientListener(client4, listener2, Optional.empty(), Optional.empty(), false, true, () -> {
               byte[] newKey = k(m, "k2-");
               client3.put(newKey, 0, 0, v(m));
               listener1.expectOnlyCreatedEvent(anyCache(), newKey);
               listener2.expectOnlyCreatedEvent(anyCache(), newKey);
               client1.put(newKey, 0, 0, v(m, "v2-"));
               listener1.expectOnlyModifiedEvent(anyCache(), newKey);
               listener2.expectOnlyModifiedEvent(anyCache(), newKey);
               client4.remove(newKey);
               listener1.expectOnlyRemovedEvent(anyCache(), newKey);
               listener2.expectOnlyRemovedEvent(anyCache(), newKey);
            });
         } finally {
            if (client4 != null) {
               client4.stop();
            }
            stopClusteredServer(newServer);
            TestingUtil.waitForNoRebalance(
                  cache(0, cacheName()), cache(1, cacheName()), cache(2, cacheName()));
         }

         client3.put(key, 0, 0, v(m, "v2-"));
         listener1.expectOnlyCreatedEvent(anyCache(), key);
         listener2.expectNoEvents(Optional.empty());
         client3.put(key, 0, 0, v(m, "v3-"));
         listener1.expectOnlyModifiedEvent(anyCache(), key);
         listener2.expectNoEvents(Optional.empty());
         client2.remove(key);
         listener1.expectOnlyRemovedEvent(anyCache(), key);
         listener2.expectNoEvents(Optional.empty());
      });
   }

   public void testFilteringInCluster(Method m) {
      HotRodClient client1 = clients().get(0);
      HotRodClient client2 = clients().get(1);
      EventLogListener listener1 = new EventLogListener();
      Optional<KeyValuePair<String, List<byte[]>>> filterFactory =
            Optional.of(new KeyValuePair<String, List<byte[]>>("accepted-key-filter-factory", Collections.emptyList()));
      byte[] key1 = k(m, "k1-");
      withClusterClientListener(client1, listener1, filterFactory, Optional.empty(), Optional.of(key1), false, () -> {
         client2.put(k(m, "k-99"), 0, 0, v(m));
         listener1.expectNoEvents(Optional.empty());
         client2.remove(k(m, "k-99"));
         listener1.expectNoEvents(Optional.empty());
         client2.put(key1, 0, 0, v(m));
         listener1.expectOnlyCreatedEvent(anyCache(), key1);
         client1.remove(key1);
         listener1.expectOnlyRemovedEvent(anyCache(), key1);
      });
   }

   public void testParameterBasedFilteringInCluster(Method m) {
      HotRodClient client1 = clients().get(0);
      HotRodClient client2 = clients().get(1);
      EventLogListener listener1 = new EventLogListener();
      byte[] dynamicAcceptedKey = new byte[]{4, 5, 6};
      Optional<KeyValuePair<String, List<byte[]>>> filterFactory = Optional.of(
            new KeyValuePair<>("accepted-key-filter-factory", Collections.singletonList(dynamicAcceptedKey)));
      withClusterClientListener(client1, listener1, filterFactory, Optional.empty(), Optional.empty(), false, () -> {
         byte[] key1 = k(m, "k1-");
         client2.put(k(m, "k-99"), 0, 0, v(m));
         listener1.expectNoEvents(Optional.empty());
         client2.remove(k(m, "k-99"));
         listener1.expectNoEvents(Optional.empty());
         client2.put(key1, 0, 0, v(m));
         listener1.expectNoEvents(Optional.empty());
         client2.put(dynamicAcceptedKey, 0, 0, v(m));
         listener1.expectOnlyCreatedEvent(anyCache(), dynamicAcceptedKey);
         client1.remove(dynamicAcceptedKey);
         listener1.expectOnlyRemovedEvent(anyCache(), dynamicAcceptedKey);
      });
   }

   public void testConversionInCluster(Method m) {
      HotRodClient client1 = clients().get(0);
      HotRodClient client2 = clients().get(1);
      EventLogListener listener1 = new EventLogListener();
      Optional<KeyValuePair<String, List<byte[]>>> converterFactory = Optional
            .of(new KeyValuePair<String, List<byte[]>>("accepted-keyvalue-converter-factory", Collections.emptyList()));
      final byte[] key1 = k(m, "k1-");
      withClusterClientListener(client1, listener1, Optional.empty(), converterFactory, Optional.of(key1), false,
                                () -> {
                                   byte[] value = v(m);

                                   byte[] key99 = k(m, "k-99");
                                   client2.put(key99, 0, 0, v(m));
                                   listener1.expectSingleCustomEvent(anyCache(), addLengthPrefix(key99));
                                   client2.put(key1, 0, 0, v(m));
                                   listener1.expectSingleCustomEvent(anyCache(), addLengthPrefix(key1, value));
                                   client2.remove(key99);
                                   listener1.expectSingleCustomEvent(anyCache(), addLengthPrefix(key99));
                                   client2.remove(key1);
                                   listener1.expectSingleCustomEvent(anyCache(), addLengthPrefix(key1));
                                });
   }

   public void testParameterBasedConversionInCluster(Method m) {
      HotRodClient client1 = clients().get(0);
      HotRodClient client2 = clients().get(1);
      EventLogListener listener1 = new EventLogListener();
      byte[] convertedKey = new byte[]{4, 5, 6};
      Optional<KeyValuePair<String, List<byte[]>>> converteFactory = Optional.of(
            new KeyValuePair<>("accepted-keyvalue-converter-factory", Collections.singletonList(new byte[]{4, 5, 6})));
      withClusterClientListener(client1, listener1, Optional.empty(), converteFactory, Optional.empty(), false, () -> {
         byte[] key1 = k(m, "k1-");
         byte[] value = v(m);

         byte[] key99 = k(m, "k-99");
         client2.put(key99, 0, 0, v(m));
         listener1.expectSingleCustomEvent(anyCache(), addLengthPrefix(key99));
         client2.put(key1, 0, 0, v(m));
         listener1.expectSingleCustomEvent(anyCache(), addLengthPrefix(key1));
         client2.put(convertedKey, 0, 0, v(m));
         listener1.expectSingleCustomEvent(anyCache(), addLengthPrefix(convertedKey, value));
         client1.remove(convertedKey);
         listener1.expectSingleCustomEvent(anyCache(), addLengthPrefix(convertedKey));
      });
   }

   public void testEventReplayAfterAddingListenerInCluster(Method m) {
      HotRodClient client1 = clients().get(0);
      HotRodClient client2 = clients().get(1);
      HotRodClient client3 = clients().get(2);
      byte[] k1 = k(m, "k1-");
      byte[] v1 = v(m, "v1-");
      byte[] k2 = k(m, "k2-");
      byte[] v2 = v(m, "v2-");
      byte[] k3 = k(m, "k3-");
      byte[] v3 = v(m, "v3-");
      client1.put(k1, 0, 0, v1);
      client2.put(k2, 0, 0, v2);
      client3.put(k3, 0, 0, v3);
      EventLogListener listener1 = new EventLogListener();
      withClientListener(client1, listener1, Optional.empty(), Optional.empty(), true, true, () -> {
         List<byte[]> keys = Arrays.asList(k1, k2, k3);
         listener1.expectUnorderedEvents(anyCache(), keys, Event.Type.CACHE_ENTRY_CREATED);
         client1.remove(k1);
         listener1.expectOnlyRemovedEvent(anyCache(), k1);
         client2.remove(k2);
         listener1.expectOnlyRemovedEvent(anyCache(), k2);
         client3.remove(k3);
         listener1.expectOnlyRemovedEvent(anyCache(), k3);
      });
   }

   public void testNoEventReplayAfterAddingListenerInCluster(Method m) {
      HotRodClient client1 = clients().get(0);
      HotRodClient client2 = clients().get(1);
      HotRodClient client3 = clients().get(2);
      byte[] k1 = k(m, "k1-");
      byte[] v1 = v(m, "v1-");
      byte[] k2 = k(m, "k2-");
      byte[] v2 = v(m, "v2-");
      byte[] k3 = k(m, "k3-");
      byte[] v3 = v(m, "v3-");
      client1.put(k1, 0, 0, v1);
      client2.put(k2, 0, 0, v2);
      client3.put(k3, 0, 0, v3);
      EventLogListener listener1 = new EventLogListener();
      withClientListener(client1, listener1, Optional.empty(), Optional.empty(), false, true, () -> {
         listener1.expectNoEvents(Optional.empty());
         client1.remove(k1);
         listener1.expectOnlyRemovedEvent(anyCache(), k1);
         client2.remove(k2);
         listener1.expectOnlyRemovedEvent(anyCache(), k2);
         client3.remove(k3);
         listener1.expectOnlyRemovedEvent(anyCache(), k3);
      });
   }

   private Cache<byte[], byte[]> anyCache() {
      return cacheManagers.get(0).<byte[], byte[]>getCache(cacheName()).getAdvancedCache();
   }

   private void withClusterClientListener(HotRodClient client, TestClientListener listener,
                                          Optional<KeyValuePair<String, List<byte[]>>> filterFactory,
                                          Optional<KeyValuePair<String, List<byte[]>>> converterFactory,
                                          Optional<byte[]> staticKey, boolean includeState, Runnable fn) {
      filters.forEach(factory -> factory.staticKey = staticKey);
      converters.forEach(factory -> factory.staticKey = staticKey);
      TestResponse response = client.addClientListener(listener, includeState, filterFactory, converterFactory, true);
      assertStatus(response, Success);
      try {
         fn.run();
      } finally {
         assertStatus(client.removeClientListener(listener.getId()), Success);
         filters.forEach(factory -> factory.staticKey = Optional.empty());
         converters.forEach(factory -> factory.staticKey = Optional.empty());
      }
   }


   public static byte[] addLengthPrefix(byte[] key) {
      byte keyLength = (byte) key.length;
      ByteBuffer buffer = ByteBuffer.allocate(keyLength + 1);
      buffer.put(keyLength);
      buffer.put(key);
      return buffer.array();
   }

   public static byte[] addLengthPrefix(byte[] key, byte[] value) {
      byte keyLength = (byte) key.length;
      byte valueLength = (byte) value.length;
      ByteBuffer buffer = ByteBuffer.allocate(keyLength + valueLength + 2);
      buffer.put(keyLength);
      buffer.put(key);
      buffer.put(valueLength);
      buffer.put(value);
      return buffer.array();
   }

   private static class AcceptedKeyFilterFactory implements CacheEventFilterFactory, Serializable, ExternalPojo {
      Optional<byte[]> staticKey = null;

      @Override
      public <K, V> CacheEventFilter<K, V> getFilter(Object[] params) {
         return (CacheEventFilter<K, V> & Serializable & ExternalPojo) ((key, oldValue, oldMetadata, newValue, newMetadata, eventType) -> {
            byte[] checkKey = staticKey.orElseGet(() -> (byte[]) params[0]);
            return Arrays.equals(checkKey, (byte[]) key);
         });
      }
   }

   private static class AcceptedKeyValueConverterFactory
         implements CacheEventConverterFactory, Serializable, ExternalPojo {
      Optional<byte[]> staticKey = null;

      @Override
      public <K, V, C> CacheEventConverter<K, V, C> getConverter(Object[] params) {
         return (CacheEventConverter<K, V, C>) (CacheEventConverter<byte[], byte[], byte[]> & Serializable & ExternalPojo) ((key, oldValue, oldMetadata, newValue, newMetadata, eventType) -> {
            byte[] checkKey = staticKey.orElseGet(() -> (byte[]) params[0]);
            if (newValue == null || !Arrays.equals(checkKey, key)) {
               return addLengthPrefix(key);
            } else {
               return addLengthPrefix(key, newValue);
            }
         });
      }
   }
}
