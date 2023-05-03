package org.infinispan.expiration.impl;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.test.skip.SkipTestNG;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.expiration.TouchMode;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Tests to make sure that when max-idle expiration occurs it occurs across the cluster
 *
 * @author William Burns
 * @since 8.0
 */
@Test(groups = "functional", testName = "expiration.impl.ClusterExpirationMaxIdleTest")
public class ClusterExpirationMaxIdleTest extends MultipleCacheManagersTest {

   protected static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   protected ControlledTimeService ts0;
   protected ControlledTimeService ts1;
   protected ControlledTimeService ts2;

   protected Cache<Object, String> cache0;
   protected Cache<Object, String> cache1;
   protected Cache<Object, String> cache2;

   private TouchMode touchMode = TouchMode.SYNC;
   protected ConfigurationBuilder configurationBuilder;

   @Override
   public Object[] factory() {
      return Arrays.stream(StorageType.values())
            .flatMap(type ->
               Stream.builder()
//                     .add(new ClusterExpirationMaxIdleTest().storageType(type).cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC))
//                     .add(new ClusterExpirationMaxIdleTest().storageType(type).cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC))
                     .add(new ClusterExpirationMaxIdleTest().storageType(type).cacheMode(CacheMode.DIST_SYNC).transactional(false))
//                     .add(new ClusterExpirationMaxIdleTest().storageType(type).cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC))
//                     .add(new ClusterExpirationMaxIdleTest().storageType(type).cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC))
//                     .add(new ClusterExpirationMaxIdleTest().storageType(type).cacheMode(CacheMode.REPL_SYNC).transactional(false))
//                     .add(new ClusterExpirationMaxIdleTest().touch(TouchMode.ASYNC).storageType(type).cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC))
//                     .add(new ClusterExpirationMaxIdleTest().touch(TouchMode.ASYNC).storageType(type).cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC))
//                     .add(new ClusterExpirationMaxIdleTest().touch(TouchMode.ASYNC).storageType(type).cacheMode(CacheMode.DIST_SYNC).transactional(false))
//                     .add(new ClusterExpirationMaxIdleTest().touch(TouchMode.ASYNC).storageType(type).cacheMode(CacheMode.REPL_SYNC).transactional(false))
                     .build()
            ).toArray();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.clustering().cacheMode(cacheMode);
      configurationBuilder.transaction().transactionMode(transactionMode()).lockingMode(lockingMode);
      configurationBuilder.expiration().disableReaper().touch(touchMode);
      if (storageType != null) {
         configurationBuilder.memory().storage(storageType);
      }
      createCluster(TestDataSCI.INSTANCE, configurationBuilder, 3);
      waitForClusterToForm();
      injectTimeServices();

      cache0 = cache(0);
      cache1 = cache(1);
      cache2 = cache(2);
   }

   protected void injectTimeServices() {
      ts0 = new ControlledTimeService(address(0));
      TestingUtil.replaceComponent(manager(0), TimeService.class, ts0, true);
      ts1 = new ControlledTimeService(address(1));
      TestingUtil.replaceComponent(manager(1), TimeService.class, ts1, true);
      ts2 = new ControlledTimeService(address(2));
      TestingUtil.replaceComponent(manager(2), TimeService.class, ts2, true);
   }

   private Object createKey(Cache<Object, String> primaryOwner, Cache<Object, String> backupOwner) {
      if (storageType == StorageType.OBJECT) {
         return getKeyForCache(primaryOwner, backupOwner);
      } else {
         // BINARY and OFF heap can't use MagicKey as they are serialized
         LocalizedCacheTopology primaryLct = primaryOwner.getAdvancedCache().getDistributionManager().getCacheTopology();
         LocalizedCacheTopology backupLct = backupOwner.getAdvancedCache().getDistributionManager().getCacheTopology();
         ThreadLocalRandom tlr = ThreadLocalRandom.current();

         int attempt = 0;
         while (true) {
            int key = tlr.nextInt();
            // We test ownership based on the stored key instance
            Object wrappedKey = primaryOwner.getAdvancedCache().getKeyDataConversion().toStorage(key);
            if (primaryLct.getDistribution(wrappedKey).isPrimary() &&
                  backupLct.getDistribution(wrappedKey).isWriteBackup()) {
               log.tracef("Found key %s for primary owner %s and backup owner %s", wrappedKey, primaryOwner, backupOwner);
               // Return the actual key not the stored one, else it will be wrapped again :(
               return key;
            }
            if (++attempt == 1_000) {
               throw new AssertionError("Unable to find key that maps to primary " + primaryOwner +
                     " and backup " + backupOwner);
            }
         }
      }
   }

   public void testMaxIdleExpiredOnBoth() throws Exception {
      Object key = createKey(cache0, cache1);
      cache1.put(key, key.toString(), -1, null, 10, MINUTES);

      incrementAllTimeServices(1, MINUTES);
      assertEquals(key.toString(), cache0.get(key));

      assertLastUsedUpdate(key, ts0.wallClockTime(), cache0, cache1);

      // It should be expired on all
      incrementAllTimeServices(11, MINUTES);

      // Both should be null
      assertNull(cache0.get(key));
      assertNull(cache1.get(key));
   }

   public void testMaxIdleExpiredOnPrimaryOwner() throws Exception {
      testMaxIdleExpiredEntryRetrieval(true);
   }

   public void testMaxIdleExpiredOnBackupOwner() throws Exception {
      testMaxIdleExpiredEntryRetrieval(false);
   }

   private void incrementAllTimeServices(long time, TimeUnit unit) {
      for (ControlledTimeService cts : Arrays.asList(ts0, ts1, ts2)) {
         cts.advance(unit.toMillis(time));
      }
   }

   private void testMaxIdleExpiredEntryRetrieval(boolean expireOnPrimary) throws Exception {
      AdvancedCache<Object, String> primaryOwner = cache0.getAdvancedCache();
      AdvancedCache<Object, String> backupOwner = cache1.getAdvancedCache();
      Object key = createKey(primaryOwner, backupOwner);
      backupOwner.put(key, key.toString(), -1, null, 10, MINUTES);

      assertEquals(key.toString(), primaryOwner.get(key));
      assertEquals(key.toString(), backupOwner.get(key));

      // Use flag CACHE_MODE_LOCAL, we don't want to go remote on accident
      AdvancedCache<Object, String> expiredCache;
      AdvancedCache<Object, String> otherCache;
      if (expireOnPrimary) {
         expiredCache = localModeCache(primaryOwner);
         otherCache = localModeCache(backupOwner);
      } else {
         expiredCache = localModeCache(backupOwner);
         otherCache = localModeCache(primaryOwner);
      }

      // Now we increment it a bit and force an access on the node that it doesn't expire on
      incrementAllTimeServices(5, MINUTES);
      assertNotNull(otherCache.get(key));

      // TODO The comment above says "we don't want to go remote on accident", but the touch command does go remotely
      assertLastUsedUpdate(key, ts1.wallClockTime(), otherCache, expiredCache);

      // Now increment enough to cause it to be expired on the other node that didn't access it
      incrementAllTimeServices(6, MINUTES);

      long targetTime = ts0.wallClockTime();
      // Now both nodes should return the value
      CacheEntry<Object, String> ce = otherCache.getCacheEntry(key);
      assertNotNull(ce);
      // Transactional cache doesn't report last access times to user
      if (transactional == Boolean.FALSE) {
         assertEquals(targetTime, ce.getLastUsed());
      }
      ce = expiredCache.getCacheEntry(key);
      assertNotNull(ce);
      if (transactional == Boolean.FALSE) {
         assertEquals(targetTime, ce.getLastUsed());
      }
   }

   private AdvancedCache<Object, String> localModeCache(AdvancedCache<Object, String> expiredCache) {
      return expiredCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_OWNERSHIP_CHECK);
   }

   private void testMaxIdleExpireExpireIteration(boolean expireOnPrimary, boolean iterateOnPrimary) {
      // Cache0 is always the primary and cache1 is backup
      Object key = createKey(cache0, cache1);
      cache1.put(key, key.toString(), -1, null, 10, SECONDS);

      ControlledTimeService expiredTimeService;
      if (expireOnPrimary) {
         expiredTimeService = ts0;
      } else {
         expiredTimeService = ts1;
      }
      expiredTimeService.advance(11, SECONDS);

      Cache<Object, String> cacheToIterate;
      if (iterateOnPrimary) {
         cacheToIterate = cache0;
      } else {
         cacheToIterate = cache1;
      }

      try (CloseableIterator<Map.Entry<Object, String>> iterator = cacheToIterate.entrySet().iterator()) {
         if (expireOnPrimary == iterateOnPrimary) {
            // Iteration only checks for expiration on the local node,
            assertFalse(iterator.hasNext());
         } else {
            assertTrue(iterator.hasNext());
            Map.Entry<Object, String> entry = iterator.next();
            assertEquals(key, entry.getKey());
            assertEquals(key.toString(), entry.getValue());
         }
      } finally {
         for (ControlledTimeService cts : Arrays.asList(ts0, ts1, ts2)) {
            if (cts != expiredTimeService) {
               cts.advance(SECONDS.toMillis(11));
            }
         }
      }
   }

   public void testMaxIdleExpirePrimaryIteratePrimary() {
      testMaxIdleExpireExpireIteration(true, true);
   }

   public void testMaxIdleExpireBackupIteratePrimary() {
      testMaxIdleExpireExpireIteration(false, true);
   }

   public void testMaxIdleExpirePrimaryIterateBackup() {
      testMaxIdleExpireExpireIteration(true, false);
   }

   public void testMaxIdleExpireBackupIterateBackup() {
      testMaxIdleExpireExpireIteration(false, false);
   }

   /**
    * This test verifies that an entry is refreshed properly when the originator thinks the entry is expired
    * but another node accessed recently, but not same timestamp
    */
   public void testMaxIdleAccessSuspectedExpiredEntryRefreshesProperly() {
      Object key = createKey(cache0, cache1);
      String value = key.toString();
      cache1.put(key, value, -1, null, 10, SECONDS);

      // Now proceed half way in the max idle period before we access it on backup node
      incrementAllTimeServices(5, SECONDS);

      // Access it on the backup to update the last access time (primary still has old access time only)
      assertEquals(value, cache1.get(key));

      // TODO The comment above says "primary still has old access time only", but the touch command does go remotely
      assertLastUsedUpdate(key, ts1.wallClockTime(), cache1, cache0);

      // Note now the entry would have been expired, if not for access above
      incrementAllTimeServices(5, SECONDS);

      assertEquals(value, cache0.get(key));

      assertLastUsedUpdate(key, ts1.wallClockTime(), cache0, cache1);

      // Now we try to access just before it expires, but it still should be available
      incrementAllTimeServices(9, SECONDS);

      assertEquals(value, cache0.get(key));
   }

   public void testPutAllExpiredEntries() {
      SkipTestNG.skipIf(cacheMode.isDistributed() && transactional,
                        "Disabled in transactional caches because of ISPN-13618");

      // Can reproduce ISPN-13549 with nKey=20_000 and no trace logs (and without the fix)
      int nKeys = 4;
      for (int i = 0; i < nKeys * 3 / 4; i++) {
         cache0.put("k" + i, "v1", -1, SECONDS, 10, SECONDS);
      }

      incrementAllTimeServices(11, SECONDS);

      Map<String, String> v2s = new HashMap<>();
      for (int i = 0; i < nKeys; i++) {
         v2s.put("k" + i, "v2");
      }
      cache0.putAll(v2s, -1, SECONDS, 10, SECONDS);
   }

   public void testGetAllExpiredEntries() {
      // Can reproduce ISPN-13549 with nKey=20_000 and no trace logs (and without the fix)
      int nKeys = 4;
      for (int i = 0; i < nKeys * 3 / 4; i++) {
         cache0.put("k" + i, "v1", -1, SECONDS, 10, SECONDS);
      }

      incrementAllTimeServices(11, SECONDS);

      Map<String, String> v1s = new HashMap<>();
      for (int i = 0; i < nKeys; i++) {
         v1s.put("k" + i, "v1");
      }
      assertEquals(Collections.emptyMap(), cache0.getAdvancedCache().getAll(v1s.keySet()));
   }

   private void assertLastUsedUpdate(Object key, long expectedLastUsed, Cache<Object, String> readCache,
                                     Cache<Object, String> otherCache) {
      Object storageKey = readCache.getAdvancedCache().getKeyDataConversion().toStorage(key);
      if (touchMode == TouchMode.SYNC) {
         assertEquals(expectedLastUsed, getLastUsed(readCache, storageKey));
         assertEquals(expectedLastUsed, getLastUsed(otherCache, storageKey));
      } else {
         // Normally the touch command is executed synchronously on the reader.
         eventuallyEquals(expectedLastUsed, () -> getLastUsed(readCache, storageKey));
         eventuallyEquals(expectedLastUsed, () -> getLastUsed(otherCache, storageKey));
      }
   }

   private long getLastUsed(Cache<Object, String> cache, Object storageKey) {
      InternalCacheEntry<Object, String> entry = cache.getAdvancedCache().getDataContainer().peek(storageKey);
      assertNotNull(entry);
      return entry.getLastUsed();
   }

   public void testMaxIdleReadNodeDiesPrimary() {
      testMaxIdleNodeDies(true);
   }

   public void testMaxIdleReadNodeDiesBackup() {
      testMaxIdleNodeDies(false);
   }

   private void testMaxIdleNodeDies(boolean isPrimary) {
      addClusterEnabledCacheManager(TestDataSCI.INSTANCE, configurationBuilder);
      waitForClusterToForm();

      Cache<Object, String> cache3 = cache(3);

      ControlledTimeService ts3 = new ControlledTimeService(address(3), ts2);
      TestingUtil.replaceComponent(manager(3), TimeService.class, ts3, true);

      Cache<Object, String> primary = isPrimary ? cache3 : cache0;
      Cache<Object, String> backup = isPrimary ? cache0 : cache3;
      Object key = createKey(primary, backup);

      backup.put(key, "max-idle", -1, SECONDS, 100, SECONDS);

      // Advance the clock so the entry is expired everywhere ("all time services" does not include ts3)
      incrementAllTimeServices(99, SECONDS);
      ts3.advance(99, SECONDS);

      assertNotNull(cache3.get(key));
      assertLastUsedUpdate(key, ts3.wallClockTime(), cache3, cache0);

      killMember(3);

      incrementAllTimeServices(2, SECONDS);

      assertNotNull(cache1.get(key));
   }

   protected MultipleCacheManagersTest touch(TouchMode touchMode) {
      this.touchMode = touchMode;
      return this;
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "touch");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), touchMode);
   }
}
