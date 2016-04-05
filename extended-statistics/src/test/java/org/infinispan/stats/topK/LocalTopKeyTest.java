package org.infinispan.stats.topK;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.interceptors.impl.TxInterceptor;
import org.infinispan.stats.AbstractTopKeyTest;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.fail;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.topK.LocalTopKeyTest")
@CleanupAfterTest
public class LocalTopKeyTest extends AbstractTopKeyTest {

   @BeforeMethod(alwaysRun = true)
   public void resetBeforeTest() {
      getTopKey(cache(0)).resetStatistics();
   }

   public void testPut(Method method) {
      final String key1 = k(method, 1);
      final String key2 = k(method, 2);

      cache(0).put(key1, "value1");
      cache(0).put(key2, "value2");

      assertTopKeyAccesses(cache(0), key1, 1, false);
      assertTopKeyAccesses(cache(0), key2, 1, false);
      assertTopKeyAccesses(cache(0), key1, 0, true);
      assertTopKeyAccesses(cache(0), key2, 0, true);

      assertLockInformation(cache(0), key1, 1, 0, 0);
      assertLockInformation(cache(0), key2, 1, 0, 0);

      assertWriteSkew(cache(0), key1, 0);
      assertWriteSkew(cache(0), key2, 0);
   }

   public void testGet(Method method) {
      final String key1 = k(method, 1);
      final String key2 = k(method, 2);

      cache(0).get(key1);
      cache(0).get(key2);

      assertTopKeyAccesses(cache(0), key1, 0, false);
      assertTopKeyAccesses(cache(0), key2, 0, false);
      assertTopKeyAccesses(cache(0), key1, 1, true);
      assertTopKeyAccesses(cache(0), key2, 1, true);

      assertLockInformation(cache(0), key1, 0, 0, 0);
      assertLockInformation(cache(0), key2, 0, 0, 0);

      assertWriteSkew(cache(0), key1, 0);
      assertWriteSkew(cache(0), key2, 0);
   }

   public void testLockFailed(Method method) throws InterruptedException, TimeoutException, ExecutionException {
      final String key = k(method, 0);

      PrepareCommandBlocker blocker = addPrepareBlockerIfAbsent(cache(0));
      blocker.reset();
      Future<Void> f = fork(() -> {
         cache(0).put(key, "value");
         return null;
      });
      blocker.awaitUntilPrepareBlocked();
      //at this point, the key is locked...
      try {
         cache(0).put(key, "value");
         Assert.fail("The key should be locked!");
      } catch (Throwable t) {
         //expected
      }
      blocker.unblock();
      f.get(30, TimeUnit.SECONDS);

      assertTopKeyAccesses(cache(0), key, 2, false);
      assertTopKeyAccesses(cache(0), key, 0, true);

      assertLockInformation(cache(0), key, 2, 1, 1);

      assertWriteSkew(cache(0), key, 0);
   }

   public void testWriteSkew(Method method) throws Exception {
      final String key = k(method, 0);

      cache(0).put(key, "init");

      tm(0).begin();
      cache(0).get(key);
      Transaction transaction = tm(0).suspend();

      cache(0).put(key, "value");

      try {
         tm(0).resume(transaction);
         cache(0).put(key, "value1");
         tm(0).commit();
         fail("The write skew should be detected");
      } catch (RollbackException t) {
         //expected
      }

      assertTopKeyAccesses(cache(0), key, 3, false);
      assertTopKeyAccesses(cache(0), key, 1, true);

      //the last put will originate an write skew
      assertLockInformation(cache(0), key, 3, 0, 0);

      assertWriteSkew(cache(0), key, 1);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL, true);
      builder.customInterceptors().addInterceptor()
            .before(TxInterceptor.class)
            .interceptor(new CacheUsageInterceptor());
      builder.versioning().enabled(true).scheme(VersioningScheme.SIMPLE);
      builder.transaction().syncCommitPhase(true).syncRollbackPhase(true);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true).lockAcquisitionTimeout(100);
      addClusterEnabledCacheManager(builder);
   }
}
