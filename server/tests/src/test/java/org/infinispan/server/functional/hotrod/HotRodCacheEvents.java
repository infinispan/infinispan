package org.infinispan.server.functional.hotrod;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.server.test.core.Common.createQueryableCache;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.wildfly.common.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.functional.extensions.entities.Entities;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@RunWith(Parameterized.class)
public class HotRodCacheEvents {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;
   private final ProtocolVersion protocolVersion;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      return Arrays.stream(ProtocolVersion.values())
            .filter(v -> v != ProtocolVersion.PROTOCOL_VERSION_20) // Listeners were introduced in 2.1
            .map(v -> new Object[]{v})
            .collect(Collectors.toList());
   }

   public HotRodCacheEvents(ProtocolVersion protocolVersion) {
      this.protocolVersion = protocolVersion;
   }

   private <K, V> RemoteCache<K, V> remoteCache() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.version(protocolVersion).addContextInitializer(Entities.INSTANCE);
      return SERVER_TEST.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
   }

   @Test
   public void testCreatedEvent() {
      new EventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.put(2, "two");
         l.expectOnlyCreatedEvent(2);
      });
   }

   @Test
   public void testModifiedEvent() {
      new EventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.put(1, "newone");
         l.expectOnlyModifiedEvent(1);
      });
   }

   @Test
   public void testRemovedEvent() {
      new EventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.remove(1);
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.remove(1);
         l.expectOnlyRemovedEvent(1);
      });
   }

   @Test
   public void testReplaceEvents() {
      new EventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.replace(1, "one");
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.replace(1, "newone");
         l.expectOnlyModifiedEvent(1);
      });
   }

   @Test
   public void testPutIfAbsentEvents() {
      new EventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.putIfAbsent(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.putIfAbsent(1, "newone");
         l.expectNoEvents();
      });
   }

   @Test
   public void testReplaceIfUnmodifiedEvents() {
      new EventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.replaceWithVersion(1, "one", 0);
         l.expectNoEvents();
         remote.putIfAbsent(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.replaceWithVersion(1, "one", 0);
         l.expectNoEvents();
         VersionedValue<?> versioned = remote.getWithMetadata(1);
         remote.replaceWithVersion(1, "one", versioned.getVersion());
         l.expectOnlyModifiedEvent(1);
      });
   }

   @Test
   public void testRemoveIfUnmodifiedEvents() {
      new EventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.removeWithVersion(1, 0);
         l.expectNoEvents();
         remote.putIfAbsent(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.removeWithVersion(1, 0);
         l.expectNoEvents();
         VersionedValue<?> versioned = remote.getWithMetadata(1);
         remote.removeWithVersion(1, versioned.getVersion());
         l.expectOnlyRemovedEvent(1);
      });
   }

   @Test
   public void testClearEvents() {
      new EventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.put(2, "two");
         l.expectOnlyCreatedEvent(2);
         remote.put(3, "three");
         l.expectOnlyCreatedEvent(3);
         remote.clear();
         l.expectUnorderedEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED, 1, 2, 3);
      });
   }

   @Test
   public void testNoEventsBeforeAddingListener() {
      RemoteCache<Integer, String> rcache = remoteCache();
      final EventLogListener<Integer, String> l = new EventLogListener<>(rcache);
      rcache.put(1, "one");
      l.expectNoEvents();
      rcache.put(1, "newone");
      l.expectNoEvents();
      rcache.remove(1);
      l.expectNoEvents();
      createUpdateRemove(l);
   }

   private void createUpdateRemove(EventLogListener<Integer, String> listener) {
      listener.accept((l, remote) -> {
         remote.put(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.put(1, "newone");
         l.expectOnlyModifiedEvent(1);
         remote.remove(1);
         l.expectOnlyRemovedEvent(1);
      });
   }

   @Test
   public void testNoEventsAfterRemovingListener() {
      final RemoteCache<Integer, String> rcache = remoteCache();
      final EventLogListener<Integer, String> l = new EventLogListener<>(rcache);
      createUpdateRemove(l);
      rcache.put(1, "one");
      l.expectNoEvents();
      rcache.put(1, "newone");
      l.expectNoEvents();
      rcache.remove(1);
      l.expectNoEvents();
   }

   @Test
   public void testSetListeners() {
      final RemoteCache<Integer, String> rcache = remoteCache();
      new EventLogListener<>(rcache).accept((l1, remote1) -> {
         Set<?> listeners1 = remote1.getListeners();
         assertEquals(1, listeners1.size());
         assertEquals(l1, listeners1.iterator().next());
         new EventLogListener<>(rcache).accept((l2, remote2) -> {
            Set<?> listeners2 = remote2.getListeners();
            assertEquals(2, listeners2.size());
            assertTrue(listeners2.contains(l1));
            assertTrue(listeners2.contains(l2));
         });
      });
      Set<Object> listeners = rcache.getListeners();
      assertEquals(0, listeners.size());
   }

   @Test
   public void testCustomTypeEvents() {
      final EventLogListener<Entities.CustomKey, String> listener = new EventLogListener<>(remoteCache());
      listener.accept((l, remote) -> {
         l.expectNoEvents();
         Entities.CustomKey key = new Entities.CustomKey(1);
         remote.put(key, "one");
         l.expectOnlyCreatedEvent(key);
         remote.replace(key, "newone");
         l.expectOnlyModifiedEvent(key);
         remote.remove(key);
         l.expectOnlyRemovedEvent(key);
      });
   }

   @Test
   public void testEventReplayAfterAddingListener() {
      RemoteCache<Integer, String> cache = remoteCache();
      final EventLogListener.WithStateEventLogListener<Integer, String> listener = new EventLogListener.WithStateEventLogListener<>(cache);
      cache.put(1, "one");
      cache.put(2, "two");
      listener.expectNoEvents();
      listener.accept((l, remote) ->
            l.expectUnorderedEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, 1, 2));
   }

   @Test
   public void testNoEventReplayAfterAddingListener() {
      RemoteCache<Integer, String> cache = remoteCache();
      final EventLogListener<Integer, String> listener = new EventLogListener<>(cache);
      cache.put(1, "one");
      cache.put(2, "two");
      listener.expectNoEvents();
      listener.accept((l, remote) -> l.expectNoEvents());
   }

   @Test
   public void testCreatedEventSkipListener() {
      new EventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(2, "two");
         l.expectNoEvents();
      });
   }

   @Test
   public void testModifiedEventSkipListener() {
      new EventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(1, "newone");
         l.expectNoEvents();
      });
   }

   @Test
   public void testRemovedEventSkipListener() {
      new EventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).remove(1);
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).remove(1);
         l.expectNoEvents();
      });
   }

   @Test
   public void testReplaceEventsSkipListener() {
      new EventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).replace(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).replace(1, "newone");
         l.expectNoEvents();
      });
   }

   @Test
   public void testPutIfAbsentEventsSkipListener() {
      new EventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).putIfAbsent(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).putIfAbsent(1, "newone");
         l.expectNoEvents();
      });
   }

   @Test
   public void testReplaceIfUnmodifiedEventsSkipListener() {
      new EventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).replaceWithVersion(1, "one", 0);
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).putIfAbsent(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).replaceWithVersion(1, "one", 0);
         l.expectNoEvents();
      });
   }

   @Test
   public void testRemoveIfUnmodifiedEventsSkipListener() {
      new EventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).removeWithVersion(1, 0);
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).putIfAbsent(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).removeWithVersion(1, 0);
         l.expectNoEvents();
      });
   }

   @Test
   public void testClearEventsSkipListener() {
      new EventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(2, "two");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(3, "three");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).clear();
         l.expectNoEvents();
      });
   }


   @Test
   public void testFilteredEvents() {
      new EventLogListener.StaticFilteredEventLogListener<>(remoteCache()).accept(new Object[]{2}, null, (l, remote) -> {
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectNoEvents();
         remote.put(2, "two");
         l.expectOnlyCreatedEvent(2);
         remote.remove(1);
         l.expectNoEvents();
         remote.remove(2);
         l.expectOnlyRemovedEvent(2);
      });
   }

   @Test
   public void testParameterBasedFiltering() {
      new EventLogListener.DynamicFilteredEventLogListener<>(remoteCache()).accept(new Object[]{3}, null, (l, remote) -> {
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectNoEvents();
         remote.put(2, "two");
         l.expectNoEvents();
         remote.put(3, "three");
         l.expectOnlyCreatedEvent(3);
      });
   }

   @Test
   public void testFilteredEventsReplay() {
      RemoteCache<Integer, String> cache = remoteCache();
      final EventLogListener.StaticFilteredEventLogWithStateListener<Integer, String> staticEventListener =
            new EventLogListener.StaticFilteredEventLogWithStateListener<>(cache);
      cache.put(1, "one");
      cache.put(2, "two");
      staticEventListener.accept(new Object[]{2}, null, (l, remote) -> {
         l.expectOnlyCreatedEvent(2);
         remote.remove(1);
         remote.remove(2);
         l.expectOnlyRemovedEvent(2);
      });
      final EventLogListener.DynamicFilteredEventLogWithStateListener<Integer, String> dynamicEventListener =
            new EventLogListener.DynamicFilteredEventLogWithStateListener<>(cache);
      cache.put(1, "one");
      cache.put(2, "two");
      cache.put(3, "three");
      dynamicEventListener.accept(new Object[]{3}, null, (l, remote) -> {
         l.expectOnlyCreatedEvent(3);
         remote.remove(1);
         remote.remove(2);
         remote.remove(3);
         l.expectOnlyRemovedEvent(3);
      });
   }

   @Test
   public void testFilteredNoEventsReplay() {
      RemoteCache<Integer, String> cache = remoteCache();
      final EventLogListener.StaticFilteredEventLogListener<Integer, String> staticEventListener =
            new EventLogListener.StaticFilteredEventLogListener<>(cache);
      cache.put(1, "one");
      cache.put(2, "two");
      staticEventListener.accept(new Object[]{2}, null, (l, remote) -> {
         l.expectNoEvents();
         remote.remove(1);
         remote.remove(2);
         l.expectOnlyRemovedEvent(2);
      });
      final EventLogListener.DynamicFilteredEventLogListener<Integer, String> dynamicEventListener =
            new EventLogListener.DynamicFilteredEventLogListener<>(cache);
      cache.put(1, "one");
      cache.put(2, "two");
      cache.put(3, "three");
      dynamicEventListener.accept(new Object[]{3}, null, (l, remote) -> {
         staticEventListener.expectNoEvents();
         remote.remove(1);
         remote.remove(2);
         remote.remove(3);
         l.expectOnlyRemovedEvent(3);
      });
   }

   /**
    * Test that the HotRod server returns an error when a ClientListener is registered with a non-existing
    * 'filterFactoryName'.
    */
   @Test(expected = HotRodClientException.class)
   public void testNonExistingConverterFactoryCustomEvents() {
      EventLogListener.NonExistingFilterFactoryListener<Integer, String> listener = new EventLogListener.NonExistingFilterFactoryListener<>(remoteCache());
      listener.accept((l, remote) -> {
      });
   }

   @Test
   public void testRawFilteredEvents() {
      final EventLogListener.RawStaticFilteredEventLogListener<Integer, String> listener =
            new EventLogListener.RawStaticFilteredEventLogListener<>(remoteCache());
      listener.accept((l, remote) -> {
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectNoEvents();
         remote.put(2, "two");
         l.expectOnlyCreatedEvent(2);
         remote.remove(1);
         l.expectNoEvents();
         remote.remove(2);
         l.expectOnlyRemovedEvent(2);
      });
   }

   @Test
   public void testCustomEvents() {
      final CustomEventLogListener.StaticCustomEventLogListener<Integer, String> listener =
            new CustomEventLogListener.StaticCustomEventLogListener<>(remoteCache());
      listener.accept((l, remote) -> {
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectCreatedEvent(new Entities.CustomEvent<>(1, "one", 0));
         remote.put(1, "newone");
         l.expectModifiedEvent(new Entities.CustomEvent<>(1, "newone", 0));
         remote.remove(1);
         l.expectRemovedEvent(new Entities.CustomEvent<>(1, null, 0));
      });
   }

   @Test
   public void testCustomEvents2() {
      final CustomEventLogListener.SimpleListener<String, String> listener = new CustomEventLogListener.SimpleListener<>(remoteCache());
      listener.accept((l, remote) -> {
         l.expectNoEvents();
         remote.put("1", "one");
         l.expectCreatedEvent("one");
      });
   }

   @Test
   public void testTimeOrderedEvents() {
      new CustomEventLogListener.StaticCustomEventLogListener<>(remoteCache()).accept((l, remote) -> {
         l.expectNoEvents();
         remote.put(1, "one");
         remote.replace(1, "newone");
         remote.replace(1, "newnewone");
         remote.replace(1, "newnewnewone");
         remote.replace(1, "newnewnewnewone");
         remote.replace(1, "newnewnewnewnewone");
         l.expectOrderedEventQueue(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      });
   }

   @Test
   public void testNoConverterFactoryCustomEvents() {
      CustomEventLogListener.NoConverterFactoryListener<Integer, String> listener = new CustomEventLogListener.NoConverterFactoryListener<>(remoteCache());
      listener.accept((l, remote) -> {
         l.expectNoEvents();
         remote.put(1, "one");
         // We don't get an event, since we don't have a converter and we only allow custom events
         l.expectNoEvents();
      });
   }

   @Test
   public void testParameterBasedConversion() {
      final CustomEventLogListener.DynamicCustomEventLogListener<Integer, String> listener =
            new CustomEventLogListener.DynamicCustomEventLogListener<>(remoteCache());
      listener.accept(null, new Object[]{2}, (l, remote) -> {
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectCreatedEvent(new Entities.CustomEvent<>(1, "one", 0));
         remote.put(2, "two");
         l.expectCreatedEvent(new Entities.CustomEvent<>(2, null, 0));
      });
   }

   @Test
   public void testConvertedEventsReplay() {
      RemoteCache<Integer, String> cache = remoteCache();
      cache.put(1, "one");
      CustomEventLogListener.StaticCustomEventLogWithStateListener<Integer, String> staticEventListener =
            new CustomEventLogListener.StaticCustomEventLogWithStateListener<>(cache);
      staticEventListener.accept((l, remote) -> l.expectCreatedEvent(new Entities.CustomEvent<>(1, "one", 0)));
      CustomEventLogListener.DynamicCustomEventWithStateLogListener<Integer, String> dynamicEventListener =
            new CustomEventLogListener.DynamicCustomEventWithStateLogListener<>(cache);
      dynamicEventListener.accept(null, new Object[]{2}, (l, remote) -> {
         l.expectCreatedEvent(new Entities.CustomEvent<>(1, "one", 0));
         remote.put(2, "two");
         l.expectCreatedEvent(new Entities.CustomEvent<>(2, null, 0));
      });
   }

   @Test
   public void testConvertedNoEventsReplay() {
      RemoteCache<Integer, String> cache = remoteCache();
      CustomEventLogListener.StaticCustomEventLogListener<Integer, String> staticListener = new CustomEventLogListener.StaticCustomEventLogListener<>(cache);
      cache.put(1, "one");
      staticListener.accept((l, remote) -> l.expectNoEvents());
      CustomEventLogListener.DynamicCustomEventLogListener<Integer, String> dynamicListener = new CustomEventLogListener.DynamicCustomEventLogListener<>(cache);
      cache.put(2, "two");
      dynamicListener.accept(null, new Object[]{2}, (l, remote) -> l.expectNoEvents());
   }

   @Test
   public void testRawCustomEvents() {
      CustomEventLogListener.RawStaticCustomEventLogListener<Integer, String> listener =
            new CustomEventLogListener.RawStaticCustomEventLogListener<>(remoteCache());
      listener.accept((l, remote) -> {
         l.expectNoEvents();
         Marshaller marshaller = remote.getRemoteCacheContainer().getMarshaller();
         Integer key = 1;
         String value = "one";
         try {
            byte[] keyBytes = marshaller.objectToByteBuffer(key);
            byte[] valBytes = marshaller.objectToByteBuffer(value);

            // Put initial value and assert converter creates a byte[] of the key + value bytes
            remote.put(key, value);
            l.expectCreatedEvent(Util.concat(keyBytes, valBytes));
            value = "newone";

            // Repeat with new value
            valBytes = marshaller.objectToByteBuffer(value);
            remote.put(key, value);
            l.expectModifiedEvent(Util.concat(keyBytes, valBytes));

            // Only keyBytes should be returned as no value in remove event
            remote.remove(key);
            l.expectRemovedEvent(keyBytes);
         } catch (InterruptedException | IOException e) {
            fail(e.getMessage());
         }
      });
   }

   @Test
   public void testEventForwarding() {
      RemoteCache<Integer, String> cache = remoteCache();
      final Integer key0 = Common.getIntKeyForServer(cache, 0);
      final Integer key1 = Common.getIntKeyForServer(cache, 1);
      final EventLogListener<Integer, String> listener = new EventLogListener<>(cache);
      listener.accept((l, remote) -> {
         l.expectNoEvents();
         remote.put(key0, "one");
         l.expectOnlyCreatedEvent(key0);
         remote.put(key1, "two");
         l.expectOnlyCreatedEvent(key1);
         remote.replace(key0, "new-one");
         l.expectOnlyModifiedEvent(key0);
         remote.replace(key1, "new-two");
         l.expectOnlyModifiedEvent(key1);
         remote.remove(key0);
         l.expectOnlyRemovedEvent(key0);
         remote.remove(key1);
         l.expectOnlyRemovedEvent(key1);
      });
   }

   @Test
   public void testFilteringInCluster() {
      RemoteCache<Integer, String> cache = remoteCache();
      final Integer key0 = Common.getIntKeyForServer(cache, 0);
      final Integer key1 = Common.getIntKeyForServer(cache, 1);
      final EventLogListener.StaticFilteredEventLogListener<Integer, String> listener = new EventLogListener.StaticFilteredEventLogListener<>(cache);
      listener.accept(new Object[]{key1}, null, (l, remote) -> {
         l.expectNoEvents();
         remote.put(key0, "one");
         l.expectNoEvents();
         remote.put(key1, "two");
         l.expectOnlyCreatedEvent(key1);
         remote.remove(key0);
         l.expectNoEvents();
         remote.remove(key1);
         l.expectOnlyRemovedEvent(key1);
      });
   }

   @Test
   public void testConversionInCluster() {
      RemoteCache<Integer, String> cache = remoteCache();
      final Integer key0 = Common.getIntKeyForServer(cache, 0);
      final Integer key1 = Common.getIntKeyForServer(cache, 1);
      final CustomEventLogListener.StaticCustomEventLogListener<Integer, String> listener = new CustomEventLogListener.StaticCustomEventLogListener<>(cache);
      listener.accept((l, remote) -> {
         l.expectNoEvents();
         remote.put(key0, "one");
         l.expectCreatedEvent(new Entities.CustomEvent<>(key0, "one", 0));
         remote.put(key1, "two");
         l.expectCreatedEvent(new Entities.CustomEvent<>(key1, "two", 0));
         remote.remove(key0);
         l.expectRemovedEvent(new Entities.CustomEvent<>(key0, null, 0));
         remote.remove(key1);
         l.expectRemovedEvent(new Entities.CustomEvent<>(key1, null, 0));
      });
   }

   @Test
   public void testFilterCustomEventsInCluster() {
      RemoteCache<Integer, String> cache = remoteCache();
      final Integer key0 = Common.getIntKeyForServer(cache, 0);
      final Integer key1 = Common.getIntKeyForServer(cache, 1);
      final CustomEventLogListener.FilterCustomEventLogListener<Integer, String> listener = new CustomEventLogListener.FilterCustomEventLogListener<>(cache);
      listener.accept(new Object[]{key0}, null, (l, remote) -> {
         remote.put(key0, "one");
         l.expectCreatedEvent(new Entities.CustomEvent<>(key0, null, 1));
         remote.put(key0, "newone");
         l.expectModifiedEvent(new Entities.CustomEvent<>(key0, null, 2));
         remote.put(key1, "two");
         l.expectCreatedEvent(new Entities.CustomEvent<>(key1, "two", 1));
         remote.put(key1, "dos");
         l.expectModifiedEvent(new Entities.CustomEvent<>(key1, "dos", 2));
         remote.remove(key0);
         l.expectRemovedEvent(new Entities.CustomEvent<>(key0, null, 3));
         remote.remove(key1);
         l.expectRemovedEvent(new Entities.CustomEvent<>(key1, null, 3));
      });
   }

   @Test
   public void testJsonEvent() {
      DataFormat jsonValues = DataFormat.builder().valueType(APPLICATION_JSON).valueMarshaller(new UTF8StringMarshaller()).build();
      RemoteCache<Integer, String> remoteCache = createQueryableCache(SERVER_TEST, false, "/proto/json.proto", "proto.JSON").withDataFormat(jsonValues);
      new EventLogListener<>(remoteCache).accept((l, cache) -> {
         l.expectNoEvents();
         cache.put(1, "{\"_type\":\"proto.JSON\",\"key\":\"one\"}");
         l.expectOnlyCreatedEvent(1);
         cache.put(2, "{\"_type\":\"proto.JSON\",\"key\":\"two\"}");
         l.expectOnlyCreatedEvent(2);
      });
   }
}
