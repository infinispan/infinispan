package org.infinispan.server.test.client.hotrod.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;

/**
 *
 * Tests for HotRod operations (especially for {@link RemoteCache} API), which can be used in
 * various security test. Some operations (e.g. #testClear(RemoteCache<String, String> remoteCache))
 * don't test functionality of the operation itself, but only checks that operation was executed
 * without throwing security exception. In general intention of these test is not to test the
 * functionality of the operations themselves, but rather test that the operation are executed
 * without throwing security exceptions.
 *
 * @author vjuranek
 * @since 7.0
 */
public class HotRodAuthzOperationTests {

   public static final String KEY1 = "key1";
   public static final String VALUE1 = "value1";
   public static final String KEY2 = "key2";
   public static final String VALUE2 = "value2";
   public static final String NON_EXISTENT_KEY = "nonExistentKey";
   public static final int ASYNC_TIMEOUT = 1; //in seconds

   public static void testClear(RemoteCache<String, String> remoteCache) {
      remoteCache.clear();
      assertTrue(remoteCache.isEmpty());
   }

   public static void testClearAsync(RemoteCache<String, String> remoteCache) throws Exception {
      CompletableFuture<Void> f = remoteCache.clearAsync();
      f.get(ASYNC_TIMEOUT, TimeUnit.SECONDS);
      assertTrue(remoteCache.isEmpty());
   }

   public static void testPutClear(RemoteCache<String, String> remoteCache) {
      remoteCache.put(KEY1, VALUE1);
      remoteCache.put(KEY2, VALUE2);
      remoteCache.clear();
      assertFalse(remoteCache.containsKey(KEY1));
      assertFalse(remoteCache.containsKey(KEY2));
      assertTrue(remoteCache.isEmpty());
   }

   public static void testPutClearAsync(RemoteCache<String, String> remoteCache) throws Exception {
      CompletableFuture<String> f1 = remoteCache.putAsync(KEY1, VALUE1);
      CompletableFuture<String> f2 = remoteCache.putAsync(KEY2, VALUE2);
      f1.get(ASYNC_TIMEOUT, TimeUnit.SECONDS);
      f2.get(ASYNC_TIMEOUT, TimeUnit.SECONDS);
      CompletableFuture<Void> c = remoteCache.clearAsync();
      c.get(ASYNC_TIMEOUT, TimeUnit.SECONDS);
      assertFalse(remoteCache.containsKey(KEY1));
      assertFalse(remoteCache.containsKey(KEY2));
      assertTrue(remoteCache.isEmpty());
   }

   public static void testContainsKey(RemoteCache<String, String> remoteCache) {
      assertFalse(remoteCache.containsKey(NON_EXISTENT_KEY));
   }

   public static void testPutContainsKey(RemoteCache<String, String> remoteCache) {
      remoteCache.put(KEY1, VALUE1);
      assertTrue(remoteCache.containsKey(KEY1));
   }

   public static void testGet(RemoteCache<String, String> remoteCache) {
      assertTrue(remoteCache.containsKey(KEY1));
      assertEquals(VALUE1, remoteCache.get(KEY1));
   }

   public static void testGetAsync(RemoteCache<String, String> remoteCache) throws Exception {
      assertTrue(remoteCache.containsKey(KEY1));
      CompletableFuture<String> f = remoteCache.getAsync(KEY1);
      assertEquals(VALUE1, f.get(ASYNC_TIMEOUT, TimeUnit.SECONDS));
   }

   public static void testGetNonExistent(RemoteCache<String, String> remoteCache) {
      assertEquals(null, remoteCache.get("nonExistentKey"));
   }

   public static void testGetNonExistentAsync(RemoteCache<String, String> remoteCache) throws Exception {
      assertEquals(null, remoteCache.getAsync("nonExistentKey").get(ASYNC_TIMEOUT, TimeUnit.SECONDS));
   }

   public static void testKeySet(RemoteCache<String, String> remoteCache) {
      remoteCache.clear();
      remoteCache.put(KEY1, VALUE1);
      remoteCache.put(KEY2, VALUE2);
      assertEquals(2, remoteCache.keySet().size());
   }

   public static void testPut(RemoteCache<String, String> remoteCache) {
      assertNull(remoteCache.put(KEY1, VALUE1));
   }

   public static void testPutAsync(RemoteCache<String, String> remoteCache) throws Exception {
      CompletableFuture<String> f = remoteCache.putAsync(KEY1, VALUE1);
      assertNull(f.get(ASYNC_TIMEOUT, TimeUnit.SECONDS));
   }

   public static void testPutGet(RemoteCache<String, String> remoteCache) {
      testPut(remoteCache);
      testGet(remoteCache);
   }

   public static void testPutGetAsync(RemoteCache<String, String> remoteCache) throws Exception {
      testPutAsync(remoteCache);
      testGetAsync(remoteCache);
   }

   public static void testPutAll(RemoteCache<String, String> remoteCache) {
      remoteCache.clear();
      Map<String, String> entries = new HashMap<String, String>(2);
      entries.put(KEY1, VALUE1);
      entries.put(KEY2, VALUE2);
      remoteCache.putAll(entries);
      assertEquals(2, remoteCache.size());
   }

   public static void testPutAllAsync(RemoteCache<String, String> remoteCache) throws Exception {
      remoteCache.clear();
      Map<String, String> entries = new HashMap<String, String>(2);
      entries.put(KEY1, VALUE1);
      entries.put(KEY2, VALUE2);
      CompletableFuture<Void> f = remoteCache.putAllAsync(entries);
      f.get(ASYNC_TIMEOUT, TimeUnit.SECONDS);
      assertEquals(2, remoteCache.size());
   }

   public static void testRemove(RemoteCache<String, String> remoteCache) {
      assertNull(remoteCache.remove(NON_EXISTENT_KEY));
   }

   public static void testRemoveAsync(RemoteCache<String, String> remoteCache) throws Exception {
      assertNull(remoteCache.removeAsync(NON_EXISTENT_KEY).get(ASYNC_TIMEOUT, TimeUnit.SECONDS));
   }

   public static void testRemoveContains(RemoteCache<String, String> remoteCache) {
      remoteCache.put(KEY1, VALUE1);
      assertTrue(remoteCache.containsKey(KEY1));
      remoteCache.remove(KEY1);
      assertFalse(remoteCache.containsKey(KEY1));
   }

   public static void testRemoveContainsAsync(RemoteCache<String, String> remoteCache) throws Exception {
      remoteCache.put(KEY1, VALUE1);
      assertTrue(remoteCache.containsKey(KEY1));
      CompletableFuture<String> f = remoteCache.removeAsync(KEY1);
      f.get(ASYNC_TIMEOUT, TimeUnit.SECONDS);
      assertFalse(remoteCache.containsKey(KEY1));
   }

   public static void testSize(RemoteCache<String, String> remoteCache) {
      assertTrue(remoteCache.size() > 0);
   }

   public static void testPutIfAbsent(RemoteCache<String, String> remoteCache) {
      remoteCache.clear();
      remoteCache.put(KEY1, VALUE1);
      assertNull(remoteCache.putIfAbsent(KEY1, "some test value"));
      assertEquals(VALUE1, remoteCache.get(KEY1));
   }

   public static void testPutIfAbsentAsync(RemoteCache<String, String> remoteCache) throws Exception {
      remoteCache.clear();
      remoteCache.put(KEY1, VALUE1);
      assertNull(remoteCache.putIfAbsentAsync(KEY1, "some test value").get(ASYNC_TIMEOUT, TimeUnit.SECONDS));
      assertEquals(VALUE1, remoteCache.getAsync(KEY1).get(ASYNC_TIMEOUT, TimeUnit.SECONDS));
   }

   public static void testReplaceWithFlag(RemoteCache<String, String> remoteCache) {
      remoteCache.put(KEY1, VALUE1);
      assertEquals(VALUE1, remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).replace(KEY1, "replace value"));
   }

   public static void testReplaceWitFlagAsync(RemoteCache<String, String> remoteCache) throws Exception {
      remoteCache.put(KEY1, VALUE1);
      assertEquals(
            VALUE1,
            remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).replaceAsync(KEY1, "replace value")
                  .get(ASYNC_TIMEOUT, TimeUnit.SECONDS));
   }

   public static void testGetWithMetadata(RemoteCache<String, String> remoteCache) {
      assertNull(remoteCache.getWithMetadata(NON_EXISTENT_KEY));
   }

   public static void testPutGetWithMetadata(RemoteCache<String, String> remoteCache) {
      remoteCache.put(KEY1, VALUE1);
      assertNotNull(remoteCache.getWithMetadata(KEY1));
   }

   public static void testGetVersioned(RemoteCache<String, String> remoteCache) {
      assertNull(remoteCache.getVersioned(NON_EXISTENT_KEY));
   }

   public static void testPutGetVersioned(RemoteCache<String, String> remoteCache) {
      remoteCache.clear();
      remoteCache.put(KEY1, VALUE1);
      VersionedValue<String> v = remoteCache.getVersioned(KEY1);
      assertEquals(VALUE1, v.getValue());
      assertTrue(v.getVersion() != 0);
   }

   public static void testReplaceWithVersioned(RemoteCache<String, String> remoteCache) {
      remoteCache.clear();
      remoteCache.put(KEY1, VALUE1);
      VersionedValue<String> v = remoteCache.getVersioned(KEY1);
      assertEquals(VALUE1, v.getValue());
      long ver = v.getVersion();
      remoteCache.replaceWithVersion(KEY1, VALUE2, ver);
      v = remoteCache.getVersioned(KEY1);
      assertEquals(VALUE2, v.getValue());
      assertTrue(ver != v.getVersion());
   }

   public static void testReplaceWithVersionAsync(RemoteCache<String, String> remoteCache) throws Exception {
      remoteCache.clear();
      remoteCache.put(KEY1, VALUE1);
      VersionedValue<String> v = remoteCache.getVersioned(KEY1);
      assertEquals(VALUE1, v.getValue());
      long ver = v.getVersion();
      CompletableFuture<Boolean> f = remoteCache.replaceWithVersionAsync(KEY1, VALUE2, ver);
      f.get(ASYNC_TIMEOUT, TimeUnit.SECONDS);
      v = remoteCache.getVersioned(KEY1);
      assertEquals(VALUE2, v.getValue());
      assertTrue(ver != v.getVersion());
   }

   public static void testRemoveWithVersion(RemoteCache<String, String> remoteCache) {
      remoteCache.clear();
      remoteCache.put(KEY1, VALUE1);
      VersionedValue<String> v = remoteCache.getVersioned(KEY1);
      assertEquals(VALUE1, v.getValue());
      long ver = v.getVersion();
      remoteCache.removeWithVersion(KEY1, ver);
      v = remoteCache.getVersioned(KEY1);
      if (v != null)
         assertTrue(ver != v.getVersion());
   }

   public static void testRemoveWithVersionAsync(RemoteCache<String, String> remoteCache) throws Exception {
      remoteCache.clear();
      remoteCache.put(KEY1, VALUE1);
      VersionedValue<String> v = remoteCache.getVersioned(KEY1);
      assertEquals(VALUE1, v.getValue());
      long ver = v.getVersion();
      CompletableFuture<Boolean> f = remoteCache.removeWithVersionAsync(KEY1, ver);
      f.get(ASYNC_TIMEOUT, TimeUnit.SECONDS);
      v = remoteCache.getVersioned(KEY1);
      if (v != null)
         assertTrue(ver != v.getVersion());
   }

   public static void testStats(RemoteCache<String, String> remoteCache) {
      ServerStatistics s = remoteCache.stats();
      assertNotNull(s);
   }

   public static void testGetRemoteCacheManager(RemoteCache<String, String> remoteCache) {
      RemoteCacheManager rcm = remoteCache.getRemoteCacheManager();
      assertNotNull(rcm);
   }

   public static void testAddGetClientListener(RemoteCache<String, String> remoteCache) {
      remoteCache.addClientListener(new NoopEventListener());
      Set<Object> s = remoteCache.getListeners();
      assertTrue(s.size() > 0);
   }

   public static void testRemoveClientListener(RemoteCache<String, String> remoteCache) {
      NoopEventListener c = new NoopEventListener();
      int initialListenerCount = remoteCache.getListeners().size();
      remoteCache.addClientListener(c);
      assertEquals(initialListenerCount + 1, remoteCache.getListeners().size());
      remoteCache.removeClientListener(c);
      assertEquals(initialListenerCount, remoteCache.getListeners().size());
   }

   @ClientListener
   public static class NoopEventListener {

      @ClientCacheEntryCreated
      public void handleCreatedEvent(ClientCacheEntryCreatedEvent<?> e) {
         //no-op
      }

      @ClientCacheEntryModified
      public void handleModifiedEvent(ClientCacheEntryModifiedEvent<?> e) {
         //no-op
      }

      @ClientCacheEntryRemoved
      public void handleRemovedEvent(ClientCacheEntryRemovedEvent<?> e) {
         //no-op
      }

   }

}
