package org.infinispan.expiration.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.MagicKey;
import org.infinispan.expiration.TouchMode;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.ControlledTimeService;
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
                    .add(new ClusterExpirationMaxIdleTest().storageType(type).cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC))
                    .add(new ClusterExpirationMaxIdleTest().storageType(type).cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC))
                    .add(new ClusterExpirationMaxIdleTest().storageType(type).cacheMode(CacheMode.DIST_SYNC).transactional(false))
                    .add(new ClusterExpirationMaxIdleTest().storageType(type).cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC))
                    .add(new ClusterExpirationMaxIdleTest().storageType(type).cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC))
                    .add(new ClusterExpirationMaxIdleTest().storageType(type).cacheMode(CacheMode.REPL_SYNC).transactional(false))
                    .add(new ClusterExpirationMaxIdleTest().storageType(type).cacheMode(CacheMode.SCATTERED_SYNC).transactional(false))
                    .add(new ClusterExpirationMaxIdleTest().touch(TouchMode.ASYNC).storageType(type).cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC))
                    .add(new ClusterExpirationMaxIdleTest().touch(TouchMode.ASYNC).storageType(type).cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC))
                    .add(new ClusterExpirationMaxIdleTest().touch(TouchMode.ASYNC).storageType(type).cacheMode(CacheMode.DIST_SYNC).transactional(false))
                    .add(new ClusterExpirationMaxIdleTest().touch(TouchMode.ASYNC).storageType(type).cacheMode(CacheMode.REPL_SYNC).transactional(false))
                    .add(new ClusterExpirationMaxIdleTest().touch(TouchMode.ASYNC).storageType(type).cacheMode(CacheMode.SCATTERED_SYNC).transactional(false))
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
      ts0 = new ControlledTimeService();
      TestingUtil.replaceComponent(manager(0), TimeService.class, ts0, true);
      ts1 = new ControlledTimeService();
      TestingUtil.replaceComponent(manager(1), TimeService.class, ts1, true);
      ts2 = new ControlledTimeService();
      TestingUtil.replaceComponent(manager(2), TimeService.class, ts2, true);
   }

   private Object createKey(Cache<Object, String> primaryOwner, Cache<Object, String> backupOwner) {
      if (storageType == StorageType.OBJECT) {
         if (cacheMode.isScattered()) {
            return new MagicKey(primaryOwner);
         } else {
            return new MagicKey(primaryOwner, backupOwner);
         }
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
                    (cacheMode.isScattered() || backupLct.getDistribution(wrappedKey).isWriteBackup())) {
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
      cache0.put(key, key.toString(), -1, null, 10, TimeUnit.MINUTES);

      incrementAllTimeServices(1, TimeUnit.MINUTES);
      assertEquals(key.toString(), cache0.get(key));
      assertEquals(key.toString(), cache1.get(key));

      if (touchMode == TouchMode.ASYNC) {
         // Wait for the async touch commands to be replicated
         Object storageKey = cache0.getAdvancedCache().getKeyDataConversion().toStorage(key);
         eventuallyEquals(ts0.wallClockTime(), () -> cache0.getAdvancedCache().getDataContainer().peek(storageKey).getLastUsed());
         eventuallyEquals(ts1.wallClockTime(), () -> cache1.getAdvancedCache().getDataContainer().peek(storageKey).getLastUsed());
         // Wait a little longer, the second get also triggers a touch command RPC
         // and we want it to reach the targets before the time services are advanced again
         Thread.sleep(10);
      }

      // It should be expired on all
      incrementAllTimeServices(11, TimeUnit.MINUTES);

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
      primaryOwner.put(key, key.toString(), -1, null, 10, TimeUnit.MINUTES);

      // Scattered can pick backup at random - so make sure we know it
      if (cacheMode == CacheMode.SCATTERED_SYNC) {
         backupOwner = findScatteredBackup(primaryOwner, key);
      }

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
      incrementAllTimeServices(5, TimeUnit.MINUTES);
      assertNotNull(otherCache.get(key));

      // TODO The comment above says "we don't want to go remote on accident", but the touch command does go remotely
      assertLastUsedUpdate(key, ts1.wallClockTime(), otherCache, expiredCache);

      // Now increment enough to cause it to be expired on the other node that didn't access it
      incrementAllTimeServices(6, TimeUnit.MINUTES);

      if (cacheMode == CacheMode.SCATTERED_SYNC) {
         // Scattered cache doesn't report last access time via getCacheEntry so we just verify the entry
         // was not removed
         String expiredValue = expiredCache.get(key);
         String otherValue = otherCache.get(key);
         assertNotNull(expiredValue);
         assertNotNull(otherValue);
      } else {
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
   }

   private AdvancedCache<Object, String> localModeCache(AdvancedCache<Object, String> expiredCache) {
      return expiredCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_OWNERSHIP_CHECK);
   }

   private AdvancedCache<Object, String> findScatteredBackup(AdvancedCache<Object, String> primaryOwner, Object key) {
      AdvancedCache<Object, String> backupOwner = null;
      for (Cache cache : caches()) {
         if (cache == primaryOwner) {
            continue;
         }
         if (localModeCache(cache.getAdvancedCache()).containsKey(key)) {
            backupOwner = cache.getAdvancedCache();
         }
      }
      return backupOwner;
   }

   private void testMaxIdleExpireExpireIteration(boolean expireOnPrimary, boolean iterateOnPrimary) {
      // Cache0 is always the primary and cache1 is backup
      Object key = createKey(cache0, cache1);
      cache1.put(key, key.toString(), -1, null, 10, TimeUnit.SECONDS);

      ControlledTimeService expiredTimeService;
      if (expireOnPrimary) {
         expiredTimeService = ts0;
      } else {
         expiredTimeService = ts1;
      }
      expiredTimeService.advance(TimeUnit.SECONDS.toMillis(11));

      Cache<Object, String> cacheToIterate;
      if (iterateOnPrimary) {
         cacheToIterate = cache0;
      } else {
         cacheToIterate = cache1;
      }

      // Iteration always works with max idle expired entries
      try (CloseableIterator<Map.Entry<Object, String>> iterator = cacheToIterate.entrySet().iterator()) {
         assertTrue(iterator.hasNext());
         Map.Entry<Object, String> entry = iterator.next();
         assertEquals(key, entry.getKey());
         assertEquals(key.toString(), entry.getValue());
      } finally {
         for (ControlledTimeService cts : Arrays.asList(ts0, ts1, ts2)) {
            if (cts != expiredTimeService) {
               cts.advance(TimeUnit.SECONDS.toMillis(11));
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
      cache0.put(key, value, -1, null, 10, TimeUnit.SECONDS);

      // Now proceed half way in the max idle period before we access it on backup node
      incrementAllTimeServices(5, TimeUnit.SECONDS);

      // Access it on the backup to update the last access time (primary still has old access time only)
      assertEquals(value, cache1.get(key));

      // TODO The comment above says "primary still has old access time only", but the touch command does go remotely
      assertLastUsedUpdate(key, ts1.wallClockTime(), cache1, cache0);

      // Note now the entry would have been expired, if not for access above
      incrementAllTimeServices(5, TimeUnit.SECONDS);

      assertEquals(value, cache0.get(key));

      assertLastUsedUpdate(key, ts1.wallClockTime(), cache0, cache1);

      // Now we try to access just before it expires, but it still should be available
      incrementAllTimeServices(9, TimeUnit.SECONDS);

      assertEquals(value, cache0.get(key));
   }

   private void assertLastUsedUpdate(Object key, long expectedLastUsed, Cache<Object, String> readCache,
                                     Cache<Object, String> otherCache) {
      Object storageKey = readCache.getAdvancedCache().getKeyDataConversion().toStorage(key);
      assertEquals(expectedLastUsed, readCache.getAdvancedCache().getDataContainer().peek(storageKey).getLastUsed());
      eventuallyEquals(expectedLastUsed,
              () -> otherCache.getAdvancedCache().getDataContainer().peek(storageKey).getLastUsed());
   }

   public void testMaxIdleReadNodeDiesPrimary() {
      // Scattered cache does not support replicating expiration metadata
      // This is to be fixed in https://issues.redhat.com/browse/ISPN-11208
      if (!cacheMode.isScattered()) {
         testMaxIdleNodeDies(c -> createKey(c, cache0));
      }
   }

   public void testMaxIdleReadNodeDiesBackup() {
      testMaxIdleNodeDies(c -> createKey(cache0, c));
   }

   private void testMaxIdleNodeDies(Function<Cache<Object, String>, Object> keyToUseFunction) {
      addClusterEnabledCacheManager(TestDataSCI.INSTANCE, configurationBuilder);
      waitForClusterToForm();

      Cache<Object, String> cache3 = cache(3);

      ControlledTimeService ts4 = new ControlledTimeService();
      TestingUtil.replaceComponent(manager(3), TimeService.class, ts4, true);

      Object key = keyToUseFunction.apply(cache3);

      // We always write to cache3 so that scattered uses it as a backup if the key isn't owned by it
      cache3.put(key, "max-idle", -1, TimeUnit.MILLISECONDS, 100, TimeUnit.MILLISECONDS);

      long justbeforeExpiration = 99;
      incrementAllTimeServices(justbeforeExpiration, TimeUnit.MILLISECONDS);
      ts4.advance(justbeforeExpiration);

      assertNotNull(cache3.get(key));

      killMember(3);

      incrementAllTimeServices(2, TimeUnit.MILLISECONDS);

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
