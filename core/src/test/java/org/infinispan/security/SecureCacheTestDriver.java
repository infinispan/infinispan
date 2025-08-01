package org.infinispan.security;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.conflict.ConflictManagerFactory;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.context.Flag;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.partitionhandling.AvailabilityMode;

import jakarta.transaction.NotSupportedException;
import jakarta.transaction.SystemException;

public class SecureCacheTestDriver {

   private final Metadata metadata;
   private final NullListener listener;
   private final CacheEventConverter<String, String, String> converter;
   private final CacheEventFilter<String, String> keyValueFilter;

   public SecureCacheTestDriver() {
      keyValueFilter = (key, oldValue, oldMetadata, newValue, newMetadata, eventType) -> true;
      converter = (key, oldValue, oldMetadata, newValue, newMetadata, eventType) -> null;
      listener = new NullListener();
      metadata = new Metadata() {

         @Override
         public long lifespan() {
            return -1;
         }

         @Override
         public long maxIdle() {
            return -1;
         }

         @Override
         public EntryVersion version() {
            return null;
         }

         @Override
         public Builder builder() {
            return null;
         }

      };
   }

   @TestCachePermission(AuthorizationPermission.READ)
   public void testIsEmpty(SecureCache<String, String> cache) {
      cache.isEmpty();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPut_Object_Object(SecureCache<String, String> cache) {
      cache.put("a", "a");
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetVersion(SecureCache<String, String> cache) {
      cache.getVersion();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutIfAbsentAsync_Object_Object_long_TimeUnit(SecureCache<String, String> cache) {
      cache.putIfAbsent("a", "a", 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPut_Object_Object_Metadata(SecureCache<String, String> cache) {
      cache.put("a", "a", metadata);
   }

   @TestCachePermission(AuthorizationPermission.LISTEN)
   public void testRemoveListener_Object(SecureCache<String, String> cache) {
      cache.removeListener(listener);
   }

   @TestCachePermission(AuthorizationPermission.LISTEN)
   public void testRemoveListenerAsync_Object(SecureCache<String, String> cache) {
      CompletionStages.join(cache.removeListenerAsync(listener));
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplace_Object_Object_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) {
      cache.replace("a", "a", 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testLock_Collection(SecureCache<String, String> cache) throws IllegalStateException, SystemException,
         NotSupportedException {
      try {
         cache.getTransactionManager().begin();
         cache.lock(Collections.singleton("a"));
      } finally {
         cache.getTransactionManager().rollback();
      }
   }

   @TestCachePermission(AuthorizationPermission.BULK_WRITE)
   public void testClear(SecureCache<String, String> cache) {
      cache.clear();
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testGetName(SecureCache<String, String> cache) {
      cache.getName();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplaceAsync_Object_Object_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) {
      cache.replaceAsync("a", "a", 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.BULK_READ)
   public void testKeySet(SecureCache<String, String> cache) {
      cache.keySet();
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetCacheConfiguration(SecureCache<String, String> cache) {
      cache.getCacheConfiguration();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutAsync_Object_Object_Metadata(SecureCache<String, String> cache) {
      cache.putAsync("a", "a", metadata);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutAsyncEntry_Object_Object_Metadata(SecureCache<String, String> cache) {
      cache.putAsyncEntry("a", "a", metadata);
   }

   @TestCachePermission(value = AuthorizationPermission.LIFECYCLE)
   public void testStop(SecureCache<String, String> cache) {
      cache.stop();
      cache.start();
   }

   @TestCachePermission(AuthorizationPermission.LISTEN)
   public void testAddListener_Object_CacheEventFilter_CacheEventConverter(SecureCache<String, String> cache) {
      cache.addListener(listener, keyValueFilter, converter);
   }

   @TestCachePermission(AuthorizationPermission.LISTEN)
   public void testAddListenerAsync_Object_CacheEventFilter_CacheEventConverter(SecureCache<String, String> cache) {
      CompletionStages.join(cache.addListenerAsync(listener, keyValueFilter, converter));
   }

   @TestCachePermission(AuthorizationPermission.LISTEN)
   public void testAddFilteredListenerAsync_Object_CacheEventFilter_CacheEventConverter_Set(SecureCache<String, String> cache) {
      CompletionStages.join(cache.addListenerAsync(listener, keyValueFilter, converter));
   }

   @TestCachePermission(AuthorizationPermission.BULK_READ)
   public void testEntrySet(SecureCache<String, String> cache) {
      cache.entrySet();
   }

   @TestCachePermission(AuthorizationPermission.READ)
   public void testContainsValue_Object(SecureCache<String, String> cache) {
      try {
         cache.containsValue("a");
      } catch (UnsupportedOperationException e) {
         // We expect this
      }
   }

   @TestCachePermission(AuthorizationPermission.READ)
   public void testContainsKey_Object(SecureCache<String, String> cache) {
      cache.containsKey("a");
   }

   @TestCachePermission(AuthorizationPermission.READ)
   public void testContainsKeyAsync_Object(SecureCache<String, String> cache) {
      cache.containsKeyAsync("a");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplace_Object_Object(SecureCache<String, String> cache) {
      cache.replace("a", "a");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplaceAsync_Object_Object_Object(SecureCache<String, String> cache) {
      cache.replaceAsync("a", "a", "b");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplaceAsync_Object_Object_Object_Metadata(SecureCache<String, String> cache) {
      cache.replaceAsync("a", "a", "b", new EmbeddedMetadata.Builder().build());
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplaceAsyncEntry_Object_Object_Metadata(SecureCache<String, String> cache) {
      cache.replaceAsyncEntry("a", "a", new EmbeddedMetadata.Builder().build());
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testGetStatus(SecureCache<String, String> cache) {
      cache.getStatus();
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testGetAvailability(SecureCache<String, String> cache) {
      cache.getAvailability();
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testSetAvailability_AvailabilityMode(SecureCache<String, String> cache) {
      cache.setAvailability(AvailabilityMode.AVAILABLE);
   }

   @TestCachePermission(AuthorizationPermission.READ)
   public void testGetAsync_Object(SecureCache<String, String> cache) {
      cache.getAsync("a");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testStartBatch(SecureCache<String, String> cache) {
      cache.startBatch();
      cache.endBatch(false);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplaceAsync_Object_Object_Object_long_TimeUnit(SecureCache<String, String> cache) {
      cache.replaceAsync("a", "a", "b", 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplace_Object_Object_Object(SecureCache<String, String> cache) {
      cache.replace("a", "a", "b");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testRemove_Object(SecureCache<String, String> cache) {
      cache.remove("a");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutIfAbsentAsync_Object_Object_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) {
      cache.putIfAbsentAsync("a", "a", 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplace_Object_Object_Object_long_TimeUnit(SecureCache<String, String> cache) {
      cache.replace("a", "a", "b", 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplace_Object_Object_long_TimeUnit(SecureCache<String, String> cache) {
      cache.replace("a", "a", 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutAsync_Object_Object(SecureCache<String, String> cache) {
      cache.putAsync("a", "a");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutIfAbsent_Object_Object_long_TimeUnit(SecureCache<String, String> cache) {
      cache.putIfAbsent("a", "a", 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testHashCode(SecureCache<String, String> cache) {
      cache.hashCode();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPut_Object_Object_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) {
      cache.put("a", "a", 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutIfAbsent_Object_Object(SecureCache<String, String> cache) {
      cache.putIfAbsent("a", "a");
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testEvict_Object(SecureCache<String, String> cache) {
      cache.evict("a");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutAsync_Object_Object_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) {
      cache.putAsync("a", "a", 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testEndBatch_boolean(SecureCache<String, String> cache) {
      cache.startBatch();
      cache.endBatch(false);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplace_Object_Object_Metadata(SecureCache<String, String> cache) {
      cache.replace("a", "a", metadata);
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testWithFlags_FlagArray(SecureCache<String, String> cache) {
      cache.withFlags(Flag.IGNORE_RETURN_VALUES);
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testWithFlags_Collection(SecureCache<String, String> cache) {
      cache.withFlags(Collections.singleton(Flag.IGNORE_RETURN_VALUES));
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testWithFlags_Flag(SecureCache<String, String> cache) {
      cache.withFlags(Flag.IGNORE_RETURN_VALUES);
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testNoFlags(SecureCache<String, String> cache) {
      cache.noFlags();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testTouch_Object_boolean(SecureCache<String, String> cache) {
      cache.touch(new Object(), true);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testTouch_Object_int_boolean(SecureCache<String, String> cache) {
      cache.touch(new Object(), 1, true);
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testTransform_Function(SecureCache<String, String> cache) {
      cache.transform(Function.identity());
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testGetBatchContainer(SecureCache<String, String> cache) {
      cache.getBatchContainer();
   }

   @TestCachePermission(AuthorizationPermission.MONITOR)
   public void testGetStats(SecureCache<String, String> cache) {
      cache.getStats();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutIfAbsentAsync_Object_Object(SecureCache<String, String> cache) {
      cache.putIfAbsentAsync("a", "a");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutIfAbsentAsync_Object_Object_Metadata(SecureCache<String, String> cache) {
      cache.putIfAbsentAsync("a", "a", new EmbeddedMetadata.Builder().build());
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutIfAbsentAsyncEntry_Object_Object_Metadata(SecureCache<String, String> cache) {
      cache.putIfAbsentAsyncEntry("a", "a", new EmbeddedMetadata.Builder().build());
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutAllAsync_Map(SecureCache<String, String> cache) {
      cache.putAllAsync(Collections.singletonMap("a", "a"));
   }

   @TestCachePermission(AuthorizationPermission.BULK_READ)
   public void testSize(SecureCache<String, String> cache) {
      cache.size();
   }

   @TestCachePermission(AuthorizationPermission.BULK_READ)
   public void testSizeAsync(SecureCache<String, String> cache) {
      cache.sizeAsync();
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetDataContainer(SecureCache<String, String> cache) {
      cache.getDataContainer();
   }

   @TestCachePermission(AuthorizationPermission.READ)
   public void testGetCacheEntry_Object(SecureCache<String, String> cache) {
      cache.getCacheEntry("a");
   }

   @TestCachePermission(AuthorizationPermission.READ)
   public void testGetCacheEntryAsync_Object(SecureCache<String, String> cache) {
      cache.getCacheEntryAsync("a");
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testGetTransactionManager(SecureCache<String, String> cache) {
      cache.getTransactionManager();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplaceAsync_Object_Object_Object_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) {
      cache.replaceAsync("a", "a", "b", 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutAll_Map_long_TimeUnit(SecureCache<String, String> cache) {
      cache.putAll(Collections.singletonMap("a", "a"), 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutForExternalRead_Object_Object(SecureCache<String, String> cache) {
      cache.putForExternalRead("a", "a");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutForExternalRead_Object_Object_long_TimeUnit(SecureCache<String, String> cache) {
      cache.putForExternalRead("a", "a", 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutForExternalRead_Object_Object_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) {
      cache.putForExternalRead("a", "a", 1, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutForExternalRead_Object_Object_Metadata(SecureCache<String, String> cache) {
      cache.putForExternalRead("a", "a", new EmbeddedMetadata.Builder().lifespan(1, TimeUnit.SECONDS).build());
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplaceAsync_Object_Object(SecureCache<String, String> cache) {
      cache.replaceAsync("a", "a");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplaceAsync_Object_Object_Metadata(SecureCache<String, String> cache) {
      cache.replaceAsync("a", "a", new EmbeddedMetadata.Builder().build());
   }

   @TestCachePermission(AuthorizationPermission.BULK_WRITE)
   public void testClearAsync(SecureCache<String, String> cache) {
      cache.clearAsync();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplace_Object_Object_Object_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) {
      cache.replace("a", "a", "b", 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutAsync_Object_Object_long_TimeUnit(SecureCache<String, String> cache) {
      cache.putAsync("a", "a", 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutAll_Map_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) {
      cache.putAll(Collections.singletonMap("a", "a"), 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplace_Object_Object_Object_Metadata(SecureCache<String, String> cache) {
      cache.replace("a", "a", "b", metadata);
   }

   @TestCachePermission(AuthorizationPermission.LIFECYCLE)
   public void testStart(SecureCache<String, String> cache) {
      cache.start();
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetDistributionManager(SecureCache<String, String> cache) {
      cache.getDistributionManager();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutAll_Map(SecureCache<String, String> cache) {
      cache.putAll(Collections.singletonMap("a", "a"));
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testEquals_Object(SecureCache<String, String> cache) {
      cache.equals(cache);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testRemove_Object_Object(SecureCache<String, String> cache) {
      cache.remove("a", "a");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testLock_ObjectArray(SecureCache<String, String> cache) throws NotSupportedException, SystemException {
      try {
         cache.getTransactionManager().begin();
         cache.lock("a");
      } finally {
         cache.getTransactionManager().rollback();
      }
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutIfAbsent_Object_Object_Metadata(SecureCache<String, String> cache) {
      cache.putIfAbsent("a", "a", metadata);
   }

   @TestCachePermission(AuthorizationPermission.LISTEN)
   public void testAddListener_Object(SecureCache<String, String> cache) {
      cache.addListener(listener);
      cache.removeListener(listener);
   }

   @TestCachePermission(AuthorizationPermission.LISTEN)
   public void testAddListenerAsync_Object(SecureCache<String, String> cache) {
      CompletionStages.join(cache.addListenerAsync(listener));
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testGetXAResource(SecureCache<String, String> cache) {
      // requires setting up a xa transaction, but since we don't test permissions, let's not bother
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetAuthorizationManager(SecureCache<String, String> cache) {
      cache.getAuthorizationManager();
   }

   @TestCachePermission(AuthorizationPermission.BULK_READ)
   public void testValues(SecureCache<String, String> cache) {
      cache.values();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testRemoveAsync_Object(SecureCache<String, String> cache) {
      cache.removeAsync("a");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testRemoveAsyncEntry_Object(SecureCache<String, String> cache) {
      cache.removeAsyncEntry("a");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPut_Object_Object_long_TimeUnit(SecureCache<String, String> cache) {
      cache.put("a", "a", 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutIfAbsent_Object_Object_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) {
      cache.putIfAbsent("a", "a", 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testGetAdvancedCache(SecureCache<String, String> cache) {
      cache.getAdvancedCache();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplaceAsync_Object_Object_long_TimeUnit(SecureCache<String, String> cache) {
      cache.replaceAsync("a", "a", 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetCacheManager(SecureCache<String, String> cache) {
      cache.getCacheManager();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutAllAsync_Map_long_TimeUnit(SecureCache<String, String> cache) {
      cache.putAllAsync(Collections.singletonMap("a", "a"), 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testGetLockManager(SecureCache<String, String> cache) {
      cache.getLockManager();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutAllAsync_Map_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) {
      cache.putAllAsync(Collections.singletonMap("a", "a"), 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutAllAsync_Map_Metadata(SecureCache<String, String> cache) {
      cache.putAllAsync(Collections.singletonMap("a", "a"), new EmbeddedMetadata.Builder().build());
   }

   @TestCachePermission(AuthorizationPermission.READ)
   public void testGet_Object(SecureCache<String, String> cache) {
      cache.get("a");
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testGetClassLoader(SecureCache<String, String> cache) {
      cache.getClassLoader();
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetRpcManager(SecureCache<String, String> cache) {
      cache.getRpcManager();
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetConflictResolutionManager(SecureCache<String, String> cache) {
      ConflictManagerFactory.get(cache);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testRemoveAsync_Object_Object(SecureCache<String, String> cache) {
      cache.removeAsync("a", "a");
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetExpirationManager(SecureCache<String, String> cache) {
      cache.getExpirationManager();
   }

   @TestCachePermission(AuthorizationPermission.READ)
   public void testGetOrDefault_Object_Object(SecureCache<String, String> cache) {
      cache.get("a");
   }

   @Listener
   public static class NullListener {

   }

   @TestCachePermission(AuthorizationPermission.BULK_READ)
   public void testGetGroup_String(SecureCache<String, String> cache) {
      cache.getGroup("someGroup");
   }

   @TestCachePermission(AuthorizationPermission.BULK_WRITE)
   public void testRemoveGroup_String(SecureCache<String, String> cache) {
      cache.removeGroup("someGroup");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutAll_Map_Metadata(SecureCache<String, String> cache) {
      cache.putAll(Collections.singletonMap("a", "a"), new EmbeddedMetadata.Builder().
            lifespan(10, TimeUnit.SECONDS).maxIdle(5, TimeUnit.SECONDS).build());
   }

   @TestCachePermission(AuthorizationPermission.BULK_READ)
   public void testGetAll_Set(SecureCache<String, String> cache) {
      cache.getAll(Collections.emptySet());
   }

   @TestCachePermission(AuthorizationPermission.BULK_READ)
   public void testGetAllAsync_Set(SecureCache<String, String> cache) {
      cache.getAllAsync(Collections.emptySet());
   }

   @TestCachePermission(AuthorizationPermission.BULK_WRITE)
   public void testGetAndPutAll_Map(SecureCache<String, String> cache) {
      cache.getAndPutAll(Collections.emptyMap());
   }

   @TestCachePermission(AuthorizationPermission.BULK_READ)
   public void testGetAllCacheEntries_Set(SecureCache<String, String> cache) {
      cache.getAllCacheEntries(Collections.emptySet());
   }

   @TestCachePermission(AuthorizationPermission.BULK_READ)
   public void testQuery_String(SecureCache<String, String> cache) {
      try {
         // we cannot invoke it without any query module linked to the core
         cache.query("select e.name from org.Employee e where e.title = 'Developer'");
      } catch (CacheException ex) {
         // catch CacheException (raised because we don't have any query module) but not a SecurityException
      }
   }

   @TestCachePermission(AuthorizationPermission.BULK_READ)
   public void testContinuousQuery(SecureCache<String, String> cache) {
      try {
         // we cannot invoke it without any query module linked to the core
         cache.continuousQuery();
      } catch (CacheException ex) {
         // catch CacheException (raised because we don't have any query module) but not a SecurityException
      }
   }

   @TestCachePermission(AuthorizationPermission.BULK_READ)
   public void testCacheEntrySet(SecureCache<String, String> cache) {
      cache.getAdvancedCache().getAllCacheEntries(Collections.emptySet());
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testRemoveLifespanExpired_Object_Object_Long(SecureCache<String, String> cache) {
      cache.getAdvancedCache().removeLifespanExpired("a", "a", null);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testRemoveMaxIdleExpired_Object_Object(SecureCache<String, String> cache) {
      cache.getAdvancedCache().removeMaxIdleExpired("a", "a");
   }

   @TestCachePermission(value = AuthorizationPermission.LIFECYCLE)
   public void testShutdown(SecureCache<String, String> cache) {
      cache.shutdown();
      cache.start();
   }

   @TestCachePermission(AuthorizationPermission.LISTEN)
   public void testAddFilteredListener_Object_CacheEventFilter_CacheEventConverter_Set(SecureCache<String, String> cache) {
      cache.addFilteredListener(listener, keyValueFilter, converter, Collections.emptySet());
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testWithSubject_Subject(SecureCache<String, String> cache) {
   }

   @TestCachePermission(AuthorizationPermission.BULK_WRITE)
   public void testLockedStream(SecureCache<String, String> cache) {
      cache.lockedStream();
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testLockAs_Object(SecureCache<String, String> cache) {
      cache.lockAs(new Object());
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testGetKeyDataConversion(SecureCache<String, String> cache) {
      cache.getKeyDataConversion();
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testGetValueDataConversion(SecureCache<String, String> cache) {
      cache.getValueDataConversion();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testCompute_Object_SerializableBiFunction(SecureCache<String, String> cache) {
      cache.compute("a", (k, v) -> "yes");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testCompute_Object_SerializableBiFunction_long_TimeUnit(SecureCache<String, String> cache) {
      cache.compute("a", (k, v) -> "yes", 1, TimeUnit.SECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testCompute_Object_SerializableBiFunction_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) {
      cache.compute("a", (k, v) -> "yes", 1, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testCompute_Object_SerializableBiFunction_Metadata(SecureCache<String, String> cache) {
      cache.compute("a", (k, v) -> "yes", metadata);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeIfPresent_Object_SerializableBiFunction(SecureCache<String, String> cache) {
      cache.computeIfPresent("a", (k, v) -> "yes");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeIfPresent_Object_SerializableBiFunction_Metadata(SecureCache<String, String> cache) {
      cache.computeIfPresent("a", (k, v) -> "yes", metadata);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeIfAbsent_Object_SerializableFunction_Metadata(SecureCache<String, String> cache) {
      cache.computeIfAbsent("b", k -> "no", metadata);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeIfAbsent_Object_SerializableFunction(SecureCache<String, String> cache) {
      cache.computeIfAbsent("b", k -> "no");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeIfAbsent_Object_SerializableFunction_long_TimeUnit(SecureCache<String, String> cache) {
      cache.computeIfAbsent("b", k -> "no", 10, TimeUnit.SECONDS, 10, TimeUnit.SECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeIfAbsent_Object_SerializableFunction_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) {
      cache.computeIfAbsent("b", k -> "no", 10, TimeUnit.SECONDS, 10, TimeUnit.SECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testMerge_Object_Object_SerializableBiFunction(SecureCache<String, String> cache) {
      cache.merge("a", "b", (k, v) -> "no");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testMerge_Object_Object_SerializableBiFunction_long_TimeUnit(SecureCache<String, String> cache) {
      cache.merge("a", "b", (k, v) -> "no", 1, TimeUnit.SECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testMerge_Object_Object_SerializableBiFunction_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) {
      cache.merge("a", "b", (k, v) -> "no", 1, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testMerge_Object_Object_SerializableBiFunction_Metadata(SecureCache<String, String> cache) {
      cache.merge("a", "b", (k, v) -> "no", metadata);
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testWithMediaType_MediaType_MediaType(SecureCache<String, String> cache) {
      cache.withMediaType(APPLICATION_OBJECT, APPLICATION_OBJECT);
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testWithStorageMediaType(SecureCache<String, String> cache) {
      cache.withStorageMediaType();
   }

   @TestCachePermission(AuthorizationPermission.LISTEN)
   public void testAddStorageFormatFilteredListener_Object_CacheEventFilter_CacheEventConverter_Set(SecureCache<String, String> cache) {
      cache.addStorageFormatFilteredListener(listener, keyValueFilter, converter, Collections.emptySet());
   }

   @TestCachePermission(AuthorizationPermission.LISTEN)
   public void testAddStorageFormatFilteredListenerAsync_Object_CacheEventFilter_CacheEventConverter_Set(SecureCache<String, String> cache) {
      CompletionStages.join(cache.addStorageFormatFilteredListenerAsync(listener, keyValueFilter, converter, Collections.emptySet()));
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeIfAbsentAsync_Object_SerializableFunction(SecureCache<String, String> cache) throws ExecutionException, InterruptedException {
      cache.computeIfAbsentAsync("b", k -> "no").get();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeIfAbsentAsync_Object_SerializableFunction_long_TimeUnit(SecureCache<String, String> cache) throws ExecutionException, InterruptedException {
      cache.computeIfAbsentAsync("b", k -> "no", 1, TimeUnit.SECONDS).get();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeIfAbsentAsync_Object_SerializableFunction_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) throws ExecutionException, InterruptedException {
      cache.computeIfAbsentAsync("b", k -> "no", 1, TimeUnit.SECONDS, 1, TimeUnit.SECONDS).get();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeIfAbsentAsync_Object_SerializableFunction_Metadata(SecureCache<String, String> cache) throws ExecutionException, InterruptedException {
      cache.computeIfAbsentAsync("b", k -> "no", metadata).get();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testMergeAsync_Object_Object_SerializableBiFunction(SecureCache<String, String> cache) throws ExecutionException, InterruptedException {
      cache.mergeAsync("a", "b", (k, v) -> "no").get();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testMergeAsync_Object_Object_SerializableBiFunction_long_TimeUnit(SecureCache<String, String> cache) throws ExecutionException, InterruptedException {
      cache.mergeAsync("a", "b", (k, v) -> "no", 1, TimeUnit.SECONDS).get();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testMergeAsync_Object_Object_SerializableBiFunction_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) throws ExecutionException, InterruptedException {
      cache.mergeAsync("a", "b", (k, v) -> "no", 1, TimeUnit.SECONDS, 1, TimeUnit.SECONDS).get();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testMergeAsync_Object_Object_SerializableBiFunction_Metadata(SecureCache<String, String> cache) throws ExecutionException, InterruptedException {
      cache.mergeAsync("a", "b", (k, v) -> "no", metadata).get();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeIfPresentAsync_Object_SerializableBiFunction(SecureCache<String, String> cache) throws ExecutionException, InterruptedException {
      cache.computeIfPresentAsync("a", (k, v) -> "yes").get();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeIfPresentAsync_Object_SerializableBiFunction_long_TimeUnit(SecureCache<String, String> cache) throws ExecutionException, InterruptedException {
      cache.computeIfPresentAsync("a", (k, v) -> "yes", 1, TimeUnit.SECONDS).get();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeIfPresentAsync_Object_SerializableBiFunction_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) throws ExecutionException, InterruptedException {
      cache.computeIfPresentAsync("a", (k, v) -> "yes", 1, TimeUnit.SECONDS, 1, TimeUnit.SECONDS).get();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeIfPresentAsync_Object_SerializableBiFunction_Metadata(SecureCache<String, String> cache) throws ExecutionException, InterruptedException {
      cache.computeIfPresentAsync("a", (k, v) -> "yes", metadata).get();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeAsync_Object_SerializableBiFunction(SecureCache<String, String> cache) throws ExecutionException, InterruptedException {
      cache.computeAsync("a", (k, v) -> "yes").get();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeAsync_Object_SerializableBiFunction_long_TimeUnit(SecureCache<String, String> cache) throws ExecutionException, InterruptedException {
      cache.computeAsync("a", (k, v) -> "yes", 1, TimeUnit.SECONDS).get();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeAsync_Object_SerializableBiFunction_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) throws ExecutionException, InterruptedException {
      cache.computeAsync("a", (k, v) -> "yes", 1, TimeUnit.SECONDS, 1, TimeUnit.SECONDS).get();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testComputeAsync_Object_SerializableBiFunction_Metadata(SecureCache<String, String> cache) throws ExecutionException, InterruptedException {
      cache.computeAsync("a", (k, v) -> "yes", metadata).get();
   }

   @TestCachePermission(AuthorizationPermission.BULK_READ)
   public void testCachePublisher(SecureCache<String, String> cache) {
      cache.cachePublisher();
   }
}
