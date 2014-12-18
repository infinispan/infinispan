package org.infinispan.client.hotrod.near;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.entryVersion;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.near.MockNearCacheService.*;
import static org.testng.AssertJUnit.*;

class AssertsNearCache<K, V> {
   final RemoteCache<K, V> remote;
   final Cache<byte[], ?> server;
   final BlockingQueue<MockEvent> events;
   final RemoteCacheManager manager;

   private AssertsNearCache(RemoteCacheManager manager, Cache<byte[], ?> server, BlockingQueue<MockEvent> events) {
      this.manager = manager;
      this.remote = manager.getCache();
      this.server = server;
      this.events = events;
   }

   static <K, V> AssertsNearCache<K, V> create(Cache<byte[], ?> server, ConfigurationBuilder builder) {
      final BlockingQueue<MockEvent> events = new ArrayBlockingQueue<>(128);
      RemoteCacheManager manager = new RemoteCacheManager(builder.build()) {
         @Override
         protected NearCacheService<K, V> createNearCacheService(NearCacheConfiguration cfg) {
            return new MockNearCacheService<K, V>(cfg, events, listenerNotifier);
         }
      };

      return new AssertsNearCache<>(manager, server, events);
   }

   static <K, V> AssertsNearCache<K, V> create(AssertsNearCache<K, V> client) {
      final BlockingQueue<MockEvent> events = new ArrayBlockingQueue<>(128);
      return new AssertsNearCache<>(client.manager, client.server, events);
   }

   AssertsNearCache<K, V> get(K key, V expected) {
      assertEquals(expected, remote.get(key));
      return this;
   }

   AssertsNearCache<K, V> getVersioned(K key, V expected) {
      VersionedValue<V> versioned = remote.getVersioned(key);
      assertEquals(expected, versioned == null ? null : versioned.getValue());
      return this;
   }

   AssertsNearCache<K, V> put(K key, V value) {
      remote.put(key, value);
      return this;
   }

   AssertsNearCache<K, V> remove(K key) {
      remote.remove(key);
      return this;
   }

   AssertsNearCache<K, V> expectNoNearEvents() {
      assertEquals(events.toString(), 0, events.size());
      return this;
   }

   AssertsNearCache<K, V> expectNearGetValueVersion(K key, V value) {
      MockGetEvent get = assertGetKeyValue(key, value);
      if (value != null) {
         long serverVersion = entryVersion(server, key);
         assertEquals(serverVersion, get.value.getVersion());
      }
      return this;
   }

   AssertsNearCache<K, V> expectNearGetValue(K key, V value) {
      assertGetKeyValue(key, value);
      return this;
   }

   AssertsNearCache<K, V> expectNearGetNull(K key) {
      MockGetEvent get = assertGetKey(key);
      assertNull(get.value);
      return this;
   }

   @SafeVarargs
   final AssertsNearCache<K, V> expectNearPut(K key, V value, AssertsNearCache<K, V>... affected) {
      expectNearPutInClient(this, key, value);
      for (AssertsNearCache<K, V> client : affected)
         expectNearPutInClient(client, key, value);

      return this;
   }

   private static <K, V> void expectNearPutInClient(AssertsNearCache<K, V> client, K key, V value) {
      MockPutEvent put = pollEvent(client.events);
      assertEquals(key, put.key);
      assertEquals(value, put.value.getValue());
   }

   AssertsNearCache<K, V> expectNearPutIfAbsent(K key, V value) {
      MockPutIfAbsentEvent put = pollEvent(events);
      assertEquals(key, put.key);
      assertEquals(value, put.value.getValue());
      return this;
   }

   @SafeVarargs
   final AssertsNearCache<K, V> expectNearRemove(K key, AssertsNearCache<K, V>... affected) {
      expectNearRemoveInClient(this, key);
      for (AssertsNearCache<K, V> client : affected)
         expectNearRemoveInClient(client, key);

      return this;
   }

   @SafeVarargs
   final AssertsNearCache<K, V> expectNearClear(AssertsNearCache<K, V>... affected) {
      expectNearClearInClient(this);
      for (AssertsNearCache<K, V> client : affected)
         expectNearClearInClient(client);

      return this;
   }

   void expectNearClearInClient(AssertsNearCache<K, V> client) {
      MockEvent clear = pollEvent(client.events);
      assertTrue("Unexpected event: " + clear, clear instanceof MockClearEvent);
   }

   void stop() {
      killRemoteCacheManager(manager);
   }

   private static <K, V> void expectNearRemoveInClient(AssertsNearCache<K, V> client, K key) {
      MockRemoveEvent remove = pollEvent(client.events);
      assertEquals(key, remove.key);
   }

   private static <E extends MockEvent> E pollEvent(BlockingQueue<MockEvent> events) {
      try {
         @SuppressWarnings("unchecked")
         E event = (E) events.poll(10, TimeUnit.SECONDS);
         assertNotNull(event);
         return event;
      } catch (InterruptedException e) {
         throw new AssertionError(e);
      }
   }

   private MockGetEvent assertGetKey(K key) {
      MockGetEvent get = pollEvent(events);
      assertEquals(key, get.key);
      return get;
   }

   private MockGetEvent assertGetKeyValue(K key, V value) {
      MockGetEvent get = assertGetKey(key);
      assertEquals(value, get.value == null ? null : get.value.getValue());
      return get;
   }

}
