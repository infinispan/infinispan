package org.infinispan.stats.topK;

import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.infinispan.test.TestingUtil.k;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.topK.LocalTopKeyTest")
@CleanupAfterTest
public class LocalTopKeyTest extends SingleCacheManagerTest {

   @BeforeMethod(alwaysRun = true)
   public void resetBeforeTest() {
      getTopKey().resetStatistics();
   }
   
   public void testPut(Method method) {
      final String key1 = k(method, 1);
      final String key2 = k(method, 2);
      
      cache.put(key1, "value1");
      cache.put(key2, "value2");

      assertTopKeyAccesses(key1, 1, false);
      assertTopKeyAccesses(key2, 1, false);
      assertTopKeyAccesses(key1, 0, true);
      assertTopKeyAccesses(key2, 0, true);

      assertLockInformation(key1, 1, 0, 0);
      assertLockInformation(key2, 1, 0, 0);

      assertWriteSkew(key1, 0);
      assertWriteSkew(key2, 0);
   }

   public void testGet(Method method) {
      final String key1 = k(method, 1);
      final String key2 = k(method, 2);

      cache.get(key1);
      cache.get(key2);

      assertTopKeyAccesses(key1, 0, false);
      assertTopKeyAccesses(key2, 0, false);
      assertTopKeyAccesses(key1, 1, true);
      assertTopKeyAccesses(key2, 1, true);

      assertLockInformation(key1, 0, 0, 0);
      assertLockInformation(key2, 0, 0, 0);

      assertWriteSkew(key1, 0);
      assertWriteSkew(key2, 0);
   }

   public void testLockFailed(Method method) throws InterruptedException, TimeoutException, ExecutionException {
      final String key = k(method, 0);

      PrepareCommandBlocker blocker = addPrepareBlockerIfAbsent(cache);
      blocker.reset();
      Future<Void> f = fork(() -> {
         cache.put(key, "value");
         return null;
      });
      blocker.awaitUntilPrepareBlocked();
      //at this point, the key is locked...
      try {
         cache.put(key, "value");
         Assert.fail("The key should be locked!");
      } catch (Throwable t) {
         //expected
      }
      blocker.unblock();
      f.get(30, TimeUnit.SECONDS);

      assertTopKeyAccesses(key, 2, false);
      assertTopKeyAccesses(key, 0, true);

      assertLockInformation(key, 2, 1, 1);

      assertWriteSkew(key, 0);
   }

   public void testWriteSkew(Method method) throws Exception {
      final String key = k(method, 0);

      cache.put(key, "init");

      tm().begin();
      cache.get(key);
      Transaction transaction = tm().suspend();

      cache.put(key, "value");

      try {
         tm().resume(transaction);
         cache.put(key, "value1");
         tm().commit();
         Assert.fail("The write skew should be detected");
      } catch (RollbackException t) {
         //expected
      }

      assertTopKeyAccesses(key, 3, false);
      assertTopKeyAccesses(key, 1, true);

      //the last put will originate an write skew
      assertLockInformation(key, 3, 0, 0);

      assertWriteSkew(key, 1);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL, true);
      builder.customInterceptors().addInterceptor()
            .before(TxInterceptor.class)
            .interceptor(new CacheUsageInterceptor());
      builder.versioning().enabled(true).scheme(VersioningScheme.SIMPLE);
      builder.transaction().syncCommitPhase(true).syncRollbackPhase(true);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true).lockAcquisitionTimeout(100);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   private CacheUsageInterceptor getTopKey() {
      for (CommandInterceptor interceptor : cache.getAdvancedCache().getInterceptorChain()) {
         if (interceptor instanceof CacheUsageInterceptor) {
            return (CacheUsageInterceptor) interceptor;
         }
      }
      throw new IllegalStateException("CacheUsageInterceptor should be in the interceptor chain");
   }

   private void assertTopKeyAccesses(String key, long expected, boolean readAccesses) {
      Long actual;
      if (readAccesses) {
         actual = getTopKey().getLocalTopGets().get(key);
      } else {
         actual = getTopKey().getLocalTopPuts().get(key);
      }
      Assert.assertEquals(actual == null ? 0 : actual, expected, "Wrong number of accesses");
   }

   private void assertWriteSkew(String key, long expected) {
      Long actual = getTopKey().getTopWriteSkewFailedKeys().get(key);
      Assert.assertEquals(actual == null ? 0 : actual, expected, "Wrong number of write skew");
   }

   private void assertTopKeyLocked(String key, long expected) {
      Long actual = getTopKey().getTopLockedKeys().get(key);
      Assert.assertEquals(actual == null ? 0 : actual, expected, "Wrong number of locked keys");
   }

   private void assertTopKeyLockContented(String key, long expected) {
      Long actual = getTopKey().getTopContendedKeys().get(key);
      Assert.assertEquals(actual == null ? 0 : actual, expected, "Wrong number of contented keys");
   }

   private void assertTopKeyLockFailed(String key, long expected) {
      Long actual = getTopKey().getTopLockFailedKeys().get(key);
      Assert.assertEquals(actual == null ? 0 : actual, expected, "Wrong number of lock failed keys");
   }

   private void assertLockInformation(String key, long locked, long contented, long failed) {
      assertTopKeyLocked(key, locked);
      assertTopKeyLockContented(key, contented);
      assertTopKeyLockFailed(key, failed);
   }

   private PrepareCommandBlocker addPrepareBlockerIfAbsent(Cache<?, ?> cache) {
      List<CommandInterceptor> chain = cache.getAdvancedCache().getInterceptorChain();

      for (CommandInterceptor commandInterceptor : chain) {
         if (commandInterceptor instanceof PrepareCommandBlocker) {
            return (PrepareCommandBlocker) commandInterceptor;
         }
      }
      PrepareCommandBlocker blocker = new PrepareCommandBlocker();
      cache.getAdvancedCache().addInterceptorBefore(blocker, TxInterceptor.class);
      return blocker;
   }

   private class PrepareCommandBlocker extends CommandInterceptor {

      private boolean unblock = false;
      private boolean prepareBlocked = false;

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         Object retVal = invokeNextInterceptor(ctx, command);
         synchronized (this) {
            prepareBlocked = true;
            notifyAll();
            while (!unblock) {
               wait();
            }
         }
         return retVal;
      }

      private synchronized void reset() {
         unblock = false;
         prepareBlocked = false;
      }

      private synchronized void unblock() {
         unblock = true;
         notifyAll();
      }

      private synchronized void awaitUntilPrepareBlocked() throws InterruptedException {
         while (!prepareBlocked) {
            wait();
         }
      }
   }
}
