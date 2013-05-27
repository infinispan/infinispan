package org.infinispan.stats;

import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.stats.topK.CacheUsageInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.BaseClusterTopKeyTest")
public abstract class BaseClusterTopKeyTest extends MultipleCacheManagersTest {

   protected final CacheMode cacheMode;
   private final int clusterSize;
   private final AtomicInteger threadCounter = new AtomicInteger(0);

   protected BaseClusterTopKeyTest(CacheMode cacheMode, int clusterSize) {
      this.cacheMode = cacheMode;
      this.clusterSize = clusterSize;
      this.cleanup = CleanupPhase.AFTER_METHOD;
   }

   public void testPut() {
      resetStreamSummary(cache(0));
      resetStreamSummary(cache(1));

      cache(0).put("key1", "value1");
      cache(0).put("key2", "value2");
      cache(1).put("key1", "value3");
      cache(1).put("key2", "value4");

      assertTopKeyAccesses(cache(0), "key1", 1, false);
      assertTopKeyAccesses(cache(0), "key2", 1, false);
      assertTopKeyAccesses(cache(0), "key1", 0, true);
      assertTopKeyAccesses(cache(0), "key2", 0, true);

      assertTopKeyAccesses(cache(1), "key1", 1, false);
      assertTopKeyAccesses(cache(1), "key2", 1, false);
      assertTopKeyAccesses(cache(1), "key1", 0, true);
      assertTopKeyAccesses(cache(1), "key2", 0, true);

      if (isPrimaryOwner(cache(0), "key1")) {
         assertLockInformation(cache(0), "key1", 2, 0, 0);
         assertLockInformation(cache(1), "key1", 0, 0, 0);
      } else {
         assertLockInformation(cache(0), "key1", 0, 0, 0);
         assertLockInformation(cache(1), "key1", 2, 0, 0);
      }

      if (isPrimaryOwner(cache(0), "key2")) {
         assertLockInformation(cache(0), "key2", 2, 0, 0);
         assertLockInformation(cache(1), "key2", 0, 0, 0);
      } else {
         assertLockInformation(cache(0), "key2", 0, 0, 0);
         assertLockInformation(cache(1), "key2", 2, 0, 0);
      }

      assertWriteSkew(cache(0), "key1", 0);
      assertWriteSkew(cache(0), "key2", 0);

      assertWriteSkew(cache(1), "key1", 0);
      assertWriteSkew(cache(1), "key2", 0);
   }

   public void testGet() {
      resetStreamSummary(cache(0));
      resetStreamSummary(cache(1));

      cache(0).get("key1");
      cache(0).get("key2");
      cache(1).get("key1");
      cache(1).get("key2");

      assertTopKeyAccesses(cache(0), "key1", 0, false);
      assertTopKeyAccesses(cache(0), "key2", 0, false);
      assertTopKeyAccesses(cache(0), "key1", 1, true);
      assertTopKeyAccesses(cache(0), "key2", 1, true);

      assertTopKeyAccesses(cache(1), "key1", 0, false);
      assertTopKeyAccesses(cache(1), "key2", 0, false);
      assertTopKeyAccesses(cache(1), "key1", 1, true);
      assertTopKeyAccesses(cache(1), "key2", 1, true);

      assertLockInformation(cache(0), "key1", 0, 0, 0);
      assertLockInformation(cache(0), "key2", 0, 0, 0);

      assertLockInformation(cache(1), "key1", 0, 0, 0);
      assertLockInformation(cache(1), "key2", 0, 0, 0);

      assertWriteSkew(cache(0), "key1", 0);
      assertWriteSkew(cache(0), "key2", 0);

      assertWriteSkew(cache(1), "key1", 0);
      assertWriteSkew(cache(1), "key2", 0);
   }

   public void testLockFailed() throws InterruptedException {
      resetStreamSummary(cache(0));
      resetStreamSummary(cache(1));

      Cache<Object, Object> primary;
      Cache<Object, Object> nonPrimary;

      if (isPrimaryOwner(cache(0), "key")) {
         primary = cache(0);
         nonPrimary = cache(1);
      } else {
         primary = cache(1);
         nonPrimary = cache(0);
      }

      PrepareCommandBlocker blocker = addPrepareBlockerIfAbsent(primary);
      blocker.reset();
      ThrowableAwareThread thread = putInOtherThread(nonPrimary, "key", "value");
      blocker.awaitUntilPrepareBlocked();
      //at this point, the key is locked...
      try {
         primary.put("key", "value");
         Assert.fail("The key should be locked!");
      } catch (Throwable t) {
         //expected
      }
      blocker.unblock();
      thread.join();
      Assert.assertNull(thread.throwable);

      assertTopKeyAccesses(cache(0), "key", 1, false);
      assertTopKeyAccesses(cache(0), "key", 0, true);

      assertTopKeyAccesses(cache(1), "key", 1, false);
      assertTopKeyAccesses(cache(1), "key", 0, true);

      assertLockInformation(primary, "key", 2, 1, 1);
      assertLockInformation(nonPrimary, "key", 0, 0, 0);

      assertWriteSkew(cache(0), "key", 0);
      assertWriteSkew(cache(1), "key", 0);
   }

   public void testWriteSkew() throws InterruptedException, SystemException, NotSupportedException {
      resetStreamSummary(cache(0));
      resetStreamSummary(cache(1));

      Cache<Object, Object> primary;
      Cache<Object, Object> nonPrimary;

      if (isPrimaryOwner(cache(0), "key")) {
         primary = cache(0);
         nonPrimary = cache(1);
      } else {
         primary = cache(1);
         nonPrimary = cache(0);
      }

      tm(primary).begin();
      primary.put("key", "value");
      Transaction transaction = tm(primary).suspend();

      primary.put("key", "value");

      try {
         tm(primary).resume(transaction);
         tm(primary).commit();
         Assert.fail("The write skew should be detected");
      } catch (Exception t) {
         //expected
      }

      assertTopKeyAccesses(primary, "key", 2, false);
      assertTopKeyAccesses(primary, "key", 0, true);

      assertTopKeyAccesses(nonPrimary, "key", 0, false);
      assertTopKeyAccesses(nonPrimary, "key", 0, true);

      assertLockInformation(primary, "key", 2, 0, 0);
      assertLockInformation(nonPrimary, "key", 0, 0, 0);

      assertWriteSkew(primary, "key", 1);
      assertWriteSkew(nonPrimary, "key", 0);
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

   protected abstract boolean isOwner(Cache<?, ?> cache, Object key);

   protected abstract boolean isPrimaryOwner(Cache<?, ?> cache, Object key);

   private ThrowableAwareThread putInOtherThread(final Cache<Object, Object> cache, final Object key, final Object value) {
      ThrowableAwareThread thread = new ThrowableAwareThread() {
         @Override
         protected final void innerRun() throws Throwable {
            cache.put(key, value);
         }
      };
      thread.start();
      return thread;
   }

   private CacheUsageInterceptor getTopKey(Cache<?, ?> cache) {
      for (CommandInterceptor interceptor : cache.getAdvancedCache().getInterceptorChain()) {
         if (interceptor instanceof CacheUsageInterceptor) {
            return (CacheUsageInterceptor) interceptor;
         }
      }
      return null;
   }

   private void resetStreamSummary(Cache<?, ?> cache) {
      CacheUsageInterceptor summaryInterceptor = getTopKey(cache);
      Assert.assertNotNull(summaryInterceptor);
      summaryInterceptor.resetStatistics();
   }

   private void assertTopKeyAccesses(Cache<?, ?> cache, Object key, long expected, boolean readAccesses) {
      CacheUsageInterceptor summaryInterceptor = getTopKey(cache);
      boolean isLocal = isOwner(cache, key);
      Long actual;
      if (readAccesses) {
         actual = isLocal ? summaryInterceptor.getLocalTopGets().get(String.valueOf(key)) :
               summaryInterceptor.getRemoteTopGets().get(String.valueOf(key));
      } else {
         actual = isLocal ? summaryInterceptor.getLocalTopPuts().get(String.valueOf(key)) :
               summaryInterceptor.getRemoteTopPuts().get(String.valueOf(key));
      }
      Assert.assertEquals(actual == null ? 0 : actual, expected, "Wrong number of accesses");
   }

   private void assertWriteSkew(Cache<?, ?> cache, Object key, long expected) {
      CacheUsageInterceptor summaryInterceptor = getTopKey(cache);
      Long actual = summaryInterceptor.getTopWriteSkewFailedKeys().get(String.valueOf(key));
      Assert.assertEquals(actual == null ? 0 : actual, expected, "Wrong number of write skew");
   }

   private void assertTopKeyLocked(Cache<?, ?> cache, Object key, long expected) {
      CacheUsageInterceptor summaryInterceptor = getTopKey(cache);
      Long actual = summaryInterceptor.getTopLockedKeys().get(String.valueOf(key));
      Assert.assertEquals(actual == null ? 0 : actual, expected, "Wrong number of locked keys");
   }

   private void assertTopKeyLockContented(Cache<?, ?> cache, Object key, long expected) {
      CacheUsageInterceptor summaryInterceptor = getTopKey(cache);
      Long actual = summaryInterceptor.getTopContendedKeys().get(String.valueOf(key));
      Assert.assertEquals(actual == null ? 0 : actual, expected, "Wrong number of contented keys");
   }

   private void assertTopKeyLockFailed(Cache<?, ?> cache, Object key, long expected) {
      CacheUsageInterceptor summaryInterceptor = getTopKey(cache);
      Long actual = summaryInterceptor.getTopLockFailedKeys().get(String.valueOf(key));
      Assert.assertEquals(actual == null ? 0 : actual, expected, "Wrong number of lock failed keys");
   }

   private void assertLockInformation(Cache<?, ?> cache, Object key, long locked, long contented, long failed) {
      assertTopKeyLocked(cache, key, locked);
      assertTopKeyLockContented(cache, key, contented);
      assertTopKeyLockFailed(cache, key, failed);
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

      public final synchronized void reset() {
         unblock = false;
         prepareBlocked = false;
      }

      public final synchronized void unblock() {
         unblock = true;
         notifyAll();
      }

      public final synchronized void awaitUntilPrepareBlocked() throws InterruptedException {
         while (!prepareBlocked) {
            wait();
         }
      }
   }

   private abstract class ThrowableAwareThread extends Thread {

      private Throwable throwable;

      protected ThrowableAwareThread() {
         super("thread-" + threadCounter.getAndIncrement() + "-" + cacheMode);
      }

      @Override
      public final void run() {
         try {
            innerRun();
         } catch (Throwable throwable) {
            this.throwable = throwable;
         }
      }

      protected abstract void innerRun() throws Throwable;
   }
}
