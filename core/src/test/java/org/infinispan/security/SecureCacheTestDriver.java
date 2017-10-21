package org.infinispan.security;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;

import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.conflict.ConflictManagerFactory;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.context.Flag;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.partitionhandling.AvailabilityMode;

public class SecureCacheTestDriver {

   private Metadata metadata;
   private NullListener listener;
   private CommandInterceptor interceptor;
   private KeyFilter<String> keyFilter;
   private CacheEventConverter<String, String, String> converter;
   private CacheEventFilter<String, String> keyValueFilter;

   public SecureCacheTestDriver() {
      interceptor = new CommandInterceptor() {
      };
      keyFilter = key -> true;
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

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testAddInterceptor_CommandInterceptor_int(SecureCache<String, String> cache) {
      cache.getAsyncInterceptorChain().addInterceptor(interceptor, 0);
      cache.getAsyncInterceptorChain().removeInterceptor(0);
   }

   @TestCachePermission(AuthorizationPermission.LISTEN)
   public void testRemoveListener_Object(SecureCache<String, String> cache) {
      cache.addListener(listener);
      cache.removeListener(listener);
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

   @TestCachePermission(value = AuthorizationPermission.LIFECYCLE, needsSecurityManager = true)
   public void testStop(SecureCache<String, String> cache) {
      cache.stop();
      cache.start();
   }

   @TestCachePermission(AuthorizationPermission.LISTEN)
   public void testAddListener_Object_CacheEventFilter_CacheEventConverter(SecureCache<String, String> cache) {
      cache.addListener(listener, keyValueFilter, converter);
   }

   @TestCachePermission(AuthorizationPermission.LISTEN)
   public void testAddListener_Object_KeyFilter(SecureCache<String, String> cache) {
      cache.addListener(listener, keyFilter);
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

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplace_Object_Object(SecureCache<String, String> cache) {
      cache.replace("a", "a");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testReplaceAsync_Object_Object_Object(SecureCache<String, String> cache) {
      cache.replaceAsync("a", "a", "b");
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

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testAddInterceptorAfter_CommandInterceptor_Class(SecureCache<String, String> cache) {
      cache.getAsyncInterceptorChain().addInterceptorAfter(interceptor, InvocationContextInterceptor.class);
      cache.getAsyncInterceptorChain().removeInterceptor(interceptor.getClass());
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

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetInvocationContextContainer(SecureCache<String, String> cache) {
      cache.getInvocationContextContainer();
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
   public void testWith_ClassLoader(SecureCache<String, String> cache) {
      cache.with(this.getClass().getClassLoader());
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testGetBatchContainer(SecureCache<String, String> cache) {
      cache.getBatchContainer();
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetStats(SecureCache<String, String> cache) {
      cache.getStats();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutIfAbsentAsync_Object_Object(SecureCache<String, String> cache) {
      cache.putIfAbsentAsync("a", "a");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutAllAsync_Map(SecureCache<String, String> cache) {
      cache.putAllAsync(Collections.singletonMap("a", "a"));
   }

   @TestCachePermission(AuthorizationPermission.BULK_READ)
   public void testSize(SecureCache<String, String> cache) {
      cache.size();
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetDataContainer(SecureCache<String, String> cache) {
      cache.getDataContainer();
   }

   @TestCachePermission(AuthorizationPermission.READ)
   public void testGetCacheEntry_Object(SecureCache<String, String> cache) {
      cache.getCacheEntry("a");
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

   @TestCachePermission(AuthorizationPermission.BULK_WRITE)
   public void testClearAsync(SecureCache<String, String> cache) {
      cache.clearAsync();
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testRemoveInterceptor_Class(SecureCache<String, String> cache) {
      cache.getAsyncInterceptorChain().removeInterceptor(interceptor.getClass());
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
   public void testApplyDelta_Object_Delta_ObjectArray(SecureCache<String, String> cache) throws Exception {
      try {
         cache.getTransactionManager().begin();

         cache.applyDelta("deltakey", new Delta() {

            @Override
            public DeltaAware merge(DeltaAware d) {
               return d;
            }
         }, "deltakey");
      } finally {
         cache.getTransactionManager().rollback();
      }
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutIfAbsent_Object_Object_Metadata(SecureCache<String, String> cache) {
      cache.putIfAbsent("a", "a", metadata);
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testRemoveInterceptor_int(SecureCache<String, String> cache) {
      cache.getAsyncInterceptorChain().addInterceptor(interceptor, 0);
      cache.getAsyncInterceptorChain().removeInterceptor(0);
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetComponentRegistry(SecureCache<String, String> cache) {
      cache.getComponentRegistry();
   }

   @TestCachePermission(AuthorizationPermission.LISTEN)
   public void testAddListener_Object(SecureCache<String, String> cache) {
      cache.addListener(listener);
      cache.removeListener(listener);
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

   @TestCachePermission(AuthorizationPermission.LISTEN)
   public void testGetListeners(SecureCache<String, String> cache) {
      cache.getListeners();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testPutAllAsync_Map_long_TimeUnit_long_TimeUnit(SecureCache<String, String> cache) {
      cache.putAllAsync(Collections.singletonMap("a", "a"), 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
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

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetInterceptorChain(SecureCache<String, String> cache) {
      cache.getInterceptorChain();
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetAsyncInterceptorChain(SecureCache<String, String> cache) {
      cache.getAsyncInterceptorChain();
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testRemoveAsync_Object_Object(SecureCache<String, String> cache) {
      cache.removeAsync("a", "a");
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetEvictionManager(SecureCache<String, String> cache) {
      cache.getEvictionManager();
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testGetExpirationManager(SecureCache<String, String> cache) {
      cache.getExpirationManager();
   }

   @TestCachePermission(AuthorizationPermission.ADMIN)
   public void testAddInterceptorBefore_CommandInterceptor_Class(SecureCache<String, String> cache) {
      cache.getAsyncInterceptorChain().addInterceptorBefore(interceptor, InvocationContextInterceptor.class);
      cache.getAsyncInterceptorChain().removeInterceptor(interceptor.getClass());
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

   @TestCachePermission(AuthorizationPermission.BULK_WRITE)
   public void testGetAndPutAll_Map(SecureCache<String, String> cache) {
      cache.getAndPutAll(Collections.emptyMap());
   }

   @TestCachePermission(AuthorizationPermission.BULK_READ)
   public void testGetAllCacheEntries_Set(SecureCache<String, String> cache) {
      cache.getAllCacheEntries(Collections.emptySet());
   }

   @TestCachePermission(AuthorizationPermission.BULK_READ)
   public void testCacheEntrySet(SecureCache<String, String> cache) {
      cache.getAdvancedCache().getAllCacheEntries(Collections.emptySet());
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testRemoveExpired_Object_Object_Long(SecureCache<String, String> cache) {
      cache.getAdvancedCache().removeExpired("a", "a", null);
   }

   @TestCachePermission(value = AuthorizationPermission.LIFECYCLE, needsSecurityManager = true)
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
   public void testWithEncoding_Class(SecureCache<String, String> cache) {
      cache.withEncoding(IdentityEncoder.class);
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testGetKeyEncoder(SecureCache<String, String> cache) {
      cache.getKeyEncoder();
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testGetValueEncoder(SecureCache<String, String> cache) {
      cache.getValueEncoder();
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testGetKeyWrapper(SecureCache<String, String> cache) {
      cache.getKeyWrapper();
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testGetValueWrapper(SecureCache<String, String> cache) {
      cache.getValueWrapper();
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testGetKeyDataConversion(SecureCache<String, String> cache) {
      cache.getKeyDataConversion();
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testGetValueDataConversion(SecureCache<String, String> cache) {
      cache.getValueDataConversion();
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testWithEncoding_Class_Class(SecureCache<String, String> cache) {
      cache.withEncoding(IdentityEncoder.class, IdentityEncoder.class);
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testWithWrapping_Class(SecureCache<String, String> cache) {
      cache.withWrapping(ByteArrayWrapper.class);
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testWithWrapping_Class_Class(SecureCache<String, String> cache) {
      cache.withWrapping(ByteArrayWrapper.class, ByteArrayWrapper.class);
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testCompute_Object_SerializableBiFunction(SecureCache<String, String> cache) {
      cache.compute("a", (k, v) -> "yes");
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
   public void testMerge_Object_Object_SerializableBiFunction(SecureCache<String, String> cache) {
      cache.merge("a", "b", (k, v) -> "no");
   }

   @TestCachePermission(AuthorizationPermission.WRITE)
   public void testMerge_Object_Object_SerializableBiFunction_Metadata(SecureCache<String, String> cache) {
      cache.merge("a", "b", (k, v) -> "no", metadata);
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testWithKeyEncoding_Class(SecureCache<String, String> cache) {
      cache.withKeyEncoding(IdentityEncoder.class);
   }

   @TestCachePermission(AuthorizationPermission.NONE)
   public void testWithMediaType_String_String(SecureCache<String, String> cache) {
      cache.withMediaType(APPLICATION_OBJECT_TYPE, APPLICATION_OBJECT_TYPE);
   }

}
