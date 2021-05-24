package org.infinispan.tx.locking;

import static java.lang.String.format;
import static org.infinispan.test.TestingUtil.extractLockManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.TimeUnit;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.concurrent.locks.KeyAwareLockPromise;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests if the leak detector unlocks the keys (ISPN-12734)
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
@Test(groups = "functional", testName = "tx.locking.LeakLockTxDetectionTest")
public class LeakLockTxDetectionTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(3);
   }

   @DataProvider(name = "params")
   public Object[][] dataProvider() {
      return new Object[][]{
            {LockingMode.OPTIMISTIC, TxOriginator.PRIMARY},
            {LockingMode.OPTIMISTIC, TxOriginator.BACKUP},
            {LockingMode.OPTIMISTIC, TxOriginator.NO_OWNER},
            {LockingMode.PESSIMISTIC, TxOriginator.PRIMARY},
            {LockingMode.PESSIMISTIC, TxOriginator.BACKUP},
            {LockingMode.PESSIMISTIC, TxOriginator.NO_OWNER}
      };
   }

   @Test(dataProvider = "params")
   public void testLockLeak(LockingMode mode, TxOriginator originator) throws Exception {
      final String cacheName = format("cache-%s", mode);
      final String key = format("key-%s-%s", mode, originator);
      final String value = format("value-%s-%s", mode, originator);

      if (manager(0).getCacheConfiguration(cacheName) == null) {
         cacheManagers.forEach(manager -> manager.defineConfiguration(cacheName, createConfig(mode).build()));
         waitForClusterToForm(cacheName);
      }

      final Cache<String, String> origCache = originatorCache(cacheName, originator);

      //lock the key with "non-existing" transaction
      lockKeyOnPrimary(cacheName, key).lock();

      //try to execute a transaction. it would fail
      assertFalse(writeKey(origCache, key, value));

      assertNoLocks(cacheName, key);

      assertTrue(writeKey(origCache, key, value)); //this should succeed
      assertNoLocks(cacheName, key);
      assertValue(cacheName, key, value);
   }

   private void assertNoLocks(String cacheName, String key) {
      for (Cache<String, String> cache : this.<String, String>caches(cacheName)) {
         eventually(format("key is locked on cache %s", address(cache)), () -> !extractLockManager(cache).isLocked(key));
      }
   }

   private void assertValue(String cacheName, String key, String value) {
      for (Cache<String, String> cache : this.<String, String>caches(cacheName)) {
         assertEquals(format("Wrong value on cache %s", address(cache)), value, cache.get(key));
      }
   }


   private boolean writeKey(Cache<String, String> cache, String key, String value) throws Exception {
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      boolean success = false;
      tm.begin();
      try {
         cache.getAdvancedCache().withFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT).put(key, value);
         tm.commit();
         success = true;
      } catch (Exception e) {
         if (tm.getStatus() != Status.STATUS_NO_TRANSACTION) {
            tm.rollback();
         }
      }
      return success;
   }

   private KeyAwareLockPromise lockKeyOnPrimary(String cacheName, String key) {
      Cache<String, String> cache = cache(0, cacheName);
      LockManager lockManager = extractLockManager(cache);
      GlobalTransaction gtx = new GlobalTransaction(cache.getCacheManager().getAddress(), false);
      return lockManager.lock(key, gtx, 0, TimeUnit.SECONDS);
   }

   private Cache<String, String> originatorCache(String cacheName, TxOriginator txOriginator) {
      switch (txOriginator) {
         case PRIMARY:
            return cache(0, cacheName);
         case BACKUP:
            return cache(1, cacheName);
         case NO_OWNER:
            return cache(2, cacheName);
         default:
            throw new IllegalStateException();
      }
   }

   private static ConfigurationBuilder createConfig(LockingMode mode) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.transaction().lockingMode(mode);
      builder.clustering().hash()
            .numSegments(1)
            .consistentHashFactory(new ControlledConsistentHashFactory.Default(0, 1));
      return builder;
   }

   private enum TxOriginator {
      PRIMARY,
      BACKUP,
      NO_OWNER
   }
}
