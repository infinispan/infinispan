package org.infinispan.client.hotrod.near;

import static org.infinispan.client.hotrod.near.MockNearCacheService.MockClearEvent;
import static org.infinispan.client.hotrod.near.MockNearCacheService.MockEvent;
import static org.infinispan.client.hotrod.near.MockNearCacheService.MockGetEvent;
import static org.infinispan.client.hotrod.near.MockNearCacheService.MockPutEvent;
import static org.infinispan.client.hotrod.near.MockNearCacheService.MockPutIfAbsentEvent;
import static org.infinispan.client.hotrod.near.MockNearCacheService.MockRemoveEvent;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.entryVersion;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.client.hotrod.configuration.NearCacheMode;

class AssertsNearCache<K, V> {
   final RemoteCache<K, V> remote;
   final Cache<byte[], ?> server;
   final BlockingQueue<MockEvent> events;
   final RemoteCacheManager manager;
   final NearCacheMode nearCacheMode;

   private AssertsNearCache(RemoteCacheManager manager, Cache<byte[], ?> server, BlockingQueue<MockEvent> events) {
      this.manager = manager;
      this.remote = manager.getCache();
      this.server = server;
      this.events = events;
      this.nearCacheMode = manager.getConfiguration().nearCache().mode();
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

   AssertsNearCache<K, V> get(K key, V expected) {
      assertEquals(expected, remote.get(key));
      return this;
   }

   AssertsNearCache<K, V> getAsync(K key, V expected) throws ExecutionException, InterruptedException {
      assertEquals(expected, remote.getAsync(key).get());
      return this;
   }

   AssertsNearCache<K, V> getVersioned(K key, V expected) {
      VersionedValue<V> versioned = remote.getVersioned(key);
      assertEquals(expected, versioned == null ? null : versioned.getValue());
      return this;
   }

   AssertsNearCache<K, V> getWithMetadata(K key, V expected) {
      MetadataValue<V> entry = remote.getWithMetadata(key);
      assertEquals(expected, entry == null ? null : entry.getValue());
      return this;
   }

   AssertsNearCache<K, V> getWithMetadataAsync(K key, V expected) throws ExecutionException, InterruptedException {
      MetadataValue<V> entry = remote.getWithMetadataAsync(key).get();
      assertEquals(expected, entry == null ? null : entry.getValue());
      return this;
   }

   AssertsNearCache<K, V> put(K key, V value) {
      remote.put(key, value);
      return this;
   }

   AssertsNearCache<K, V> putAsync(K key, V value) throws ExecutionException, InterruptedException {
      remote.putAsync(key, value).get();
      return this;
   }

   AssertsNearCache<K, V> putAsync(K key, V value, long time, TimeUnit timeUnit) throws ExecutionException, InterruptedException {
      remote.putAsync(key, value, time, timeUnit).get();
      return this;
   }

   AssertsNearCache<K, V> putAsync(K key, V value, long time, TimeUnit timeUnit, long idle, TimeUnit idleTimeUnit) throws ExecutionException, InterruptedException {
      remote.putAsync(key, value, time, timeUnit, idle, idleTimeUnit).get();
      return this;
   }

   AssertsNearCache<K, V> remove(K key) {
      remote.remove(key);
      return this;
   }

   AssertsNearCache<K, V> removeAsync(K key) throws ExecutionException, InterruptedException {
      remote.removeAsync(key).get();
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
      expectLocalNearRemoveInClient(this, key);
      for (AssertsNearCache<K, V> client : affected)
         expectRemoteNearRemoveInClient(client, key);

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

   private static <K, V> void expectLocalNearRemoveInClient(AssertsNearCache<K, V> client, K key) {
      if (client.nearCacheMode.invalidated()) {
         // Preemptive remove
         MockRemoveEvent preemptiveRemove = pollEvent(client.events);
         assertEquals(key, preemptiveRemove.key);
      }
      // Remote event remove
      MockRemoveEvent remoteRemove = pollEvent(client.events);
      assertEquals(key, remoteRemove.key);
   }

   private static <K, V> void expectRemoteNearRemoveInClient(AssertsNearCache<K, V> client, K key) {
      // Remote event remove
      MockRemoveEvent remoteRemove = pollEvent(client.events);
      assertEquals(key, remoteRemove.key);
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
