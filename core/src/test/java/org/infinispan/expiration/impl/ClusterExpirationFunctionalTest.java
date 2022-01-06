package org.infinispan.expiration.impl;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.triangle.BackupWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.test.skip.SkipTestNG;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.ControlledRpcManager;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.SkipException;
import org.testng.annotations.Test;

/**
 * Tests to make sure that when expiration occurs it occurs across the cluster
 *
 * @author William Burns
 * @since 8.0
 */
@Test(groups = "functional", testName = "expiration.impl.ClusterExpirationFunctionalTest")
public class ClusterExpirationFunctionalTest extends MultipleCacheManagersTest {

   protected static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   protected ControlledTimeService ts0;
   protected ControlledTimeService ts1;
   protected ControlledTimeService ts2;

   protected Cache<Object, String> cache0;
   protected Cache<Object, String> cache1;
   protected Cache<Object, String> cache2;

   protected ConfigurationBuilder configurationBuilder;

   @Override
   public Object[] factory() {
      return Arrays.stream(StorageType.values())
            .flatMap(type ->
               Stream.builder()
                     .add(new ClusterExpirationFunctionalTest().storageType(type).cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC))
                     .add(new ClusterExpirationFunctionalTest().storageType(type).cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC))
                     .add(new ClusterExpirationFunctionalTest().storageType(type).cacheMode(CacheMode.DIST_SYNC).transactional(false))
                     .add(new ClusterExpirationFunctionalTest().storageType(type).cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC))
                     .add(new ClusterExpirationFunctionalTest().storageType(type).cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC))
                     .add(new ClusterExpirationFunctionalTest().storageType(type).cacheMode(CacheMode.REPL_SYNC).transactional(false))
                     .add(new ClusterExpirationFunctionalTest().storageType(type).cacheMode(CacheMode.SCATTERED_SYNC).transactional(false))
                     .build()
            ).toArray();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.clustering().cacheMode(cacheMode);
      configurationBuilder.transaction().transactionMode(transactionMode()).lockingMode(lockingMode);
      configurationBuilder.expiration().disableReaper();
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

   public void testLifespanExpiredOnPrimaryOwner() throws Exception {
      testLifespanExpiredEntryRetrieval(cache0, cache1, ts0, true);
   }

   public void testLifespanExpiredOnBackupOwner() throws Exception {
      testLifespanExpiredEntryRetrieval(cache0, cache1, ts1, false);
   }

   private void testLifespanExpiredEntryRetrieval(Cache<Object, String> primaryOwner, Cache<Object, String> backupOwner,
           ControlledTimeService timeService, boolean expireOnPrimary) throws Exception {
      Object key = createKey(primaryOwner, backupOwner);
      primaryOwner.put(key, key.toString(), 10, TimeUnit.MILLISECONDS);

      assertEquals(key.toString(), primaryOwner.get(key));
      assertEquals(key.toString(), backupOwner.get(key));

      // Now we expire on cache0, it should still exist on cache1
      // Note this has to be within the buffer in RemoveExpiredCommand (100 ms the time of this commit)
      timeService.advance(11);

      Cache<?, ?> expiredCache;
      Cache<?, ?> otherCache;
      if (expireOnPrimary) {
         expiredCache = primaryOwner;
         otherCache = backupOwner;
      } else {
         expiredCache = backupOwner;
         otherCache = primaryOwner;
      }

      Cache<?, ?> other;
      if (cacheMode.isScattered()) {
         // In scattered cache the read would go to primary always
         other = otherCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_OWNERSHIP_CHECK);
      } else {
         other = otherCache;
      }
      assertEquals(key.toString(), other.get(key));

      // By calling get on an expired key it will remove it all over
      Object expiredValue = expiredCache.get(key);
      // In scattered mode, the get goes from backup to remote node where the time has not passed yet,
      // therefore it is not expired. We don't check expiration on local node after receiving the response.
      if (cacheMode.isScattered() && !expireOnPrimary) {
         assertEquals(key.toString(), expiredValue);
      }  else {
         assertNull(expiredValue);
         // This should be expired on the other node soon - note expiration is done asynchronously on a get
         eventually(() -> !other.containsKey(key), 10, TimeUnit.SECONDS);
      }
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

   public void testLifespanExpiredOnBoth() {
      Object key = createKey(cache0, cache1);
      cache0.put(key, key.toString(), 10, TimeUnit.MINUTES);

      assertEquals(key.toString(), cache0.get(key));
      assertEquals(key.toString(), cache1.get(key));

      // Now we expire on cache0, it should still exist on cache1
      ts0.advance(TimeUnit.MINUTES.toMillis(10) + 1);
      ts1.advance(TimeUnit.MINUTES.toMillis(10) + 1);

      // Both should be null
      assertNull(cache0.get(key));
      assertNull(cache1.get(key));
   }

   public void testMaxIdleExpiredOnBoth() {
      Object key = createKey(cache0, cache1);
      cache0.put(key, key.toString(), -1, null, 10, TimeUnit.MINUTES);

      assertEquals(key.toString(), cache0.get(key));
      assertEquals(key.toString(), cache1.get(key));

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

      assertEquals(key.toString(), primaryOwner.get(key));
      assertEquals(key.toString(), backupOwner.get(key));

      AdvancedCache<Object, String> expiredCache;
      AdvancedCache<Object, String> otherCache;
      if (expireOnPrimary) {
         expiredCache = primaryOwner;
         otherCache = backupOwner;
      } else {
         expiredCache = backupOwner;
         otherCache = primaryOwner;
      }

      // We don't want to go remote on accident
      expiredCache = expiredCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_OWNERSHIP_CHECK);
      otherCache = otherCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_OWNERSHIP_CHECK);

      // Now we increment it a bit and force an access on the node that it doesn't expire on
      incrementAllTimeServices(5, TimeUnit.MINUTES);
      assertNotNull(otherCache.get(key));

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

      // Note now the entry would have been expired, if not for access above
      incrementAllTimeServices(5, TimeUnit.SECONDS);

      assertEquals(value, cache0.get(key));

      // Now we try to access just before it expires, but it still should be available
      incrementAllTimeServices(9, TimeUnit.SECONDS);

      assertEquals(value, cache0.get(key));
   }

   public void testPutAllExpiredEntries() {
      SkipTestNG.skipIf(cacheMode.isDistributed() && transactional,
                        "Disabled in transactional caches because of ISPN-13618");
      SkipTestNG.skipIf(cacheMode.isScattered(), "Disabled in scattered caches because of ISPN-13619");

      Map<String, String> v1s = new HashMap<>();
      // Can reproduce ISPN-13549 with nKey=20_000 and no trace logs (and without the fix)
      int nKeys = 4;
      for (int i = 0; i < nKeys; i++) {
         v1s.put("k" + i, "v1");
      }
      Map<String, String> v2s = new HashMap<>();
      for (int i = 0; i < nKeys; i++) {
         v2s.put("k" + i, "v2");
      }
      cache0.putAll(v1s, -1, SECONDS, 10, SECONDS);

      incrementAllTimeServices(11, SECONDS);

      cache0.putAll(v2s, -1, SECONDS, 10, SECONDS);
   }

   @Test(groups = "unstable", description = "https://issues.redhat.com/browse/ISPN-11422")
   public void testWriteExpiredEntry() {
      String key = "key";
      String value = "value";

      for (int i = 0; i < 100; ++i) {
         Cache<Object, String> cache = cache0;
         Object prev = cache.get(key);
         if (prev == null) {
            prev = cache.putIfAbsent(key, value, 1, TimeUnit.SECONDS);

            // Should be guaranteed to be null
            assertNull(prev);

            // We should always have a value still
            assertNotNull(cache.get(key));
         }

         long secondOneMilliAdvanced = TimeUnit.SECONDS.toMillis(1);

         ts0.advance(secondOneMilliAdvanced);
         ts1.advance(secondOneMilliAdvanced);
         ts2.advance(secondOneMilliAdvanced);
      }
   }

   // Simpler test for https://issues.redhat.com/browse/ISPN-11422
//   public void testBackupExpirationWritePrimary() {
//      testExpirationButOnBackupDuringWrite(true);
//   }
//
//   public void testBackupExpirationWriteBackup() {
//      testExpirationButOnBackupDuringWrite(false);
//   }
//
//   private void testExpirationButOnBackupDuringWrite(boolean primary) {
//      Object key = createKey(cache0, cache1);
//      String value = key.toString();
//      assertNull(cache0.put(key, value, 10, TimeUnit.SECONDS));
//
//      // Advance the backup so it is expired there
//      ts1.advance(TimeUnit.SECONDS.toMillis(11));
//
//      assertEquals(value, ((primary ? cache0 : cache1).put(key, "replacement-value")));
//   }

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

   public void testPrimaryNotExpiredButBackupWas() throws InterruptedException, ExecutionException, TimeoutException {
      if (transactional || cacheMode == CacheMode.SCATTERED_SYNC) {
         throw new SkipException("Test isn't supported in transactional mode or scattered cache");
      }
      Object key = createKey(cache0, cache1);
      String value = key.toString();
      cache0.put(key, value,10, TimeUnit.SECONDS);

      final ControlledRpcManager controlledRpcManager = ControlledRpcManager.replaceRpcManager(cache0);
      Class<? extends ReplicableCommand> commandToExpect;
      if (cacheMode == CacheMode.DIST_SYNC) {
         controlledRpcManager.excludeCommands(PutKeyValueCommand.class);
         commandToExpect = BackupWriteCommand.class;
      } else {
         commandToExpect = PutKeyValueCommand.class;
      }

      try {
         Future<String> result = fork(() -> cache0.put(key, value + "-expire-backup"));

         ControlledRpcManager.BlockedRequest<? extends ReplicableCommand> blockedRequest = controlledRpcManager.expectCommand(commandToExpect);

         incrementAllTimeServices(11, TimeUnit.SECONDS);

         ControlledRpcManager.SentRequest sentRequest = blockedRequest.send();

         if (sentRequest != null) {
            sentRequest.expectAllResponses().receive();
         }

         assertEquals(value, result.get(10, TimeUnit.SECONDS));
      } finally {
         controlledRpcManager.revertRpcManager();
      }
      assertEquals(value + "-expire-backup", cache0.get(key));
      assertEquals(value + "-expire-backup", cache1.get(key));
      assertEquals(value + "-expire-backup", cache2.get(key));
   }
}
