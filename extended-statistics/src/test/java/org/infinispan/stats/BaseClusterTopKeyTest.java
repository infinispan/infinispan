package org.infinispan.stats;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.stats.topK.CacheUsageInterceptor;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.infinispan.distribution.DistributionTestHelper.addressOf;
import static org.infinispan.test.TestingUtil.k;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional")
@CleanupAfterTest
public abstract class BaseClusterTopKeyTest extends AbstractTopKeyTest {

   private final CacheMode cacheMode;
   private final int clusterSize;

   protected BaseClusterTopKeyTest(CacheMode cacheMode, int clusterSize) {
      this.cacheMode = cacheMode;
      this.clusterSize = clusterSize;
   }

   @BeforeMethod(alwaysRun = true)
   public void resetBeforeTest() {
      caches().forEach(cache -> getTopKey(cache).resetStatistics());
   }

   public void testPut(Method method) {
      final String key1 = k(method, 1);
      final String key2 = k(method, 2);

      cache(0).put(key1, "value1");
      cache(0).put(key2, "value2");

      assertNoLocks(key1);
      assertNoLocks(key2);

      cache(1).put(key1, "value3");
      cache(1).put(key2, "value4");

      assertTopKeyAccesses(cache(0), key1, 1, false);
      assertTopKeyAccesses(cache(0), key2, 1, false);
      assertTopKeyAccesses(cache(0), key1, 0, true);
      assertTopKeyAccesses(cache(0), key2, 0, true);

      assertTopKeyAccesses(cache(1), key1, 1, false);
      assertTopKeyAccesses(cache(1), key2, 1, false);
      assertTopKeyAccesses(cache(1), key1, 0, true);
      assertTopKeyAccesses(cache(1), key2, 0, true);

      if (isPrimaryOwner(cache(0), key1)) {
         assertLockInformation(cache(0), key1, 2, 0, 0);
         assertLockInformation(cache(1), key1, 0, 0, 0);
      } else {
         assertLockInformation(cache(0), key1, 0, 0, 0);
         assertLockInformation(cache(1), key1, 2, 0, 0);
      }

      if (isPrimaryOwner(cache(0), key2)) {
         assertLockInformation(cache(0), key2, 2, 0, 0);
         assertLockInformation(cache(1), key2, 0, 0, 0);
      } else {
         assertLockInformation(cache(0), key2, 0, 0, 0);
         assertLockInformation(cache(1), key2, 2, 0, 0);
      }

      assertWriteSkew(cache(0), key1, 0);
      assertWriteSkew(cache(0), key2, 0);

      assertWriteSkew(cache(1), key1, 0);
      assertWriteSkew(cache(1), key2, 0);
   }

   public void testGet(Method method) {
      final String key1 = k(method, 1);
      final String key2 = k(method, 2);

      cache(0).get(key1);
      cache(0).get(key2);
      cache(1).get(key1);
      cache(1).get(key2);

      assertTopKeyAccesses(cache(0), key1, 0, false);
      assertTopKeyAccesses(cache(0), key2, 0, false);
      assertTopKeyAccesses(cache(0), key1, 1, true);
      assertTopKeyAccesses(cache(0), key2, 1, true);

      assertTopKeyAccesses(cache(1), key1, 0, false);
      assertTopKeyAccesses(cache(1), key2, 0, false);
      assertTopKeyAccesses(cache(1), key1, 1, true);
      assertTopKeyAccesses(cache(1), key2, 1, true);

      assertLockInformation(cache(0), key1, 0, 0, 0);
      assertLockInformation(cache(0), key2, 0, 0, 0);

      assertLockInformation(cache(1), key1, 0, 0, 0);
      assertLockInformation(cache(1), key2, 0, 0, 0);

      assertWriteSkew(cache(0), key1, 0);
      assertWriteSkew(cache(0), key2, 0);

      assertWriteSkew(cache(1), key1, 0);
      assertWriteSkew(cache(1), key2, 0);
   }

   public void testLockFailed(Method method) throws InterruptedException, TimeoutException, ExecutionException {
      final String key = k(method, 0);

      Cache<Object, Object> primary;
      Cache<Object, Object> nonPrimary;

      if (isPrimaryOwner(cache(0), key)) {
         primary = cache(0);
         nonPrimary = cache(1);
      } else {
         primary = cache(1);
         nonPrimary = cache(0);
      }

      PrepareCommandBlocker blocker = addPrepareBlockerIfAbsent(primary);
      blocker.reset();
      Future<Void> f = fork(() -> {
         nonPrimary.put(key, "value");
         return null;
      });
      blocker.awaitUntilPrepareBlocked();
      //at this point, the key is locked...
      try {
         primary.put(key, "value");
         Assert.fail("The key should be locked!");
      } catch (Throwable t) {
         //expected
      }
      blocker.unblock();
      f.get(30, TimeUnit.SECONDS);

      assertTopKeyAccesses(cache(0), key, 1, false);
      assertTopKeyAccesses(cache(0), key, 0, true);

      assertTopKeyAccesses(cache(1), key, 1, false);
      assertTopKeyAccesses(cache(1), key, 0, true);

      assertLockInformation(primary, key, 2, 1, 1);
      assertLockInformation(nonPrimary, key, 0, 0, 0);

      assertWriteSkew(cache(0), key, 0);
      assertWriteSkew(cache(1), key, 0);
   }

   public void testWriteSkew(Method method) throws InterruptedException, SystemException, NotSupportedException {
      final String key = k(method, 0);

      Cache<Object, Object> primary;
      Cache<Object, Object> nonPrimary;

      if (isPrimaryOwner(cache(0), key)) {
         primary = cache(0);
         nonPrimary = cache(1);
      } else {
         primary = cache(1);
         nonPrimary = cache(0);
      }

      tm(primary).begin();
      primary.put(key, "value");
      Transaction transaction = tm(primary).suspend();

      primary.put(key, "value");

      try {
         tm(primary).resume(transaction);
         tm(primary).commit();
         Assert.fail("The write skew should be detected");
      } catch (Exception t) {
         //expected
      }

      assertTopKeyAccesses(primary, key, 2, false);
      assertTopKeyAccesses(primary, key, 0, true);

      assertTopKeyAccesses(nonPrimary, key, 0, false);
      assertTopKeyAccesses(nonPrimary, key, 0, true);

      assertLockInformation(primary, key, 2, 0, 0);
      assertLockInformation(nonPrimary, key, 0, 0, 0);

      assertWriteSkew(primary, key, 1);
      assertWriteSkew(nonPrimary, key, 0);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < clusterSize; ++i) {
         ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, true);
         builder.customInterceptors().addInterceptor()
               .before(TxInterceptor.class)
               .interceptor(new CacheUsageInterceptor());
         builder.versioning().enabled(true).scheme(VersioningScheme.SIMPLE);
         builder.transaction().syncCommitPhase(true).syncRollbackPhase(true);
         builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true).lockAcquisitionTimeout(100);
         addClusterEnabledCacheManager(builder);
      }
      waitForClusterToForm();
   }

   protected boolean isPrimaryOwner(Cache<?, ?> cache, Object key) {
      DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
      return dm.getPrimaryLocation(key).equals(addressOf(cache));
   }

   private void assertNoLocks(String key) {
      for (Cache<?, ?> cache : caches()) {
         assertEventuallyNotLocked(cache, key);
      }
   }
}
