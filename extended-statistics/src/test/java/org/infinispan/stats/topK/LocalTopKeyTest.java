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
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.topK.LocalTopKeyTest")
public class LocalTopKeyTest extends SingleCacheManagerTest {

   protected final CacheMode cacheMode;
   private final AtomicInteger threadCounter = new AtomicInteger(0);

   protected LocalTopKeyTest() {
      this.cacheMode = CacheMode.LOCAL;
      this.cleanup = CleanupPhase.AFTER_METHOD;
   }

   public void testPut() {
      resetStreamSummary(cache());

      cache().put("key1", "value1");
      cache().put("key2", "value2");

      assertTopKeyAccesses(cache(), "key1", 1, false);
      assertTopKeyAccesses(cache(), "key2", 1, false);
      assertTopKeyAccesses(cache(), "key1", 0, true);
      assertTopKeyAccesses(cache(), "key2", 0, true);

      assertLockInformation(cache(), "key1", 1, 0, 0);
      assertLockInformation(cache(), "key2", 1, 0, 0);

      assertWriteSkew(cache(), "key1", 0);
      assertWriteSkew(cache(), "key2", 0);
   }

   public void testGet() {
      resetStreamSummary(cache());

      cache().get("key1");
      cache().get("key2");

      assertTopKeyAccesses(cache(), "key1", 0, false);
      assertTopKeyAccesses(cache(), "key2", 0, false);
      assertTopKeyAccesses(cache(), "key1", 1, true);
      assertTopKeyAccesses(cache(), "key2", 1, true);

      assertLockInformation(cache(), "key1", 0, 0, 0);
      assertLockInformation(cache(), "key2", 0, 0, 0);

      assertWriteSkew(cache(), "key1", 0);
      assertWriteSkew(cache(), "key2", 0);
   }

   public void testLockFailed() throws InterruptedException {
      resetStreamSummary(cache());

      PrepareCommandBlocker blocker = addPrepareBlockerIfAbsent(cache());
      blocker.reset();
      ThrowableAwareThread thread = putInOtherThread(cache(), "key", "value");
      blocker.awaitUntilPrepareBlocked();
      //at this point, the key is locked...
      try {
         cache().put("key", "value");
         Assert.fail("The key should be locked!");
      } catch (Throwable t) {
         //expected
      }
      blocker.unblock();
      thread.join();
      Assert.assertNull(thread.throwable);

      assertTopKeyAccesses(cache(), "key", 2, false);
      assertTopKeyAccesses(cache(), "key", 0, true);

      assertLockInformation(cache(), "key", 2, 1, 1);

      assertWriteSkew(cache(), "key", 0);
   }

   public void testWriteSkew() throws Exception {
      resetStreamSummary(cache());

      cache().put("key", "init");

      tm().begin();
      cache().get("key");
      Transaction transaction = tm().suspend();

      cache().put("key", "value");

      try {
         tm().resume(transaction);
         cache().put("key", "value1");
         tm().commit();
         Assert.fail("The write skew should be detected");
      } catch (RollbackException t) {
         //expected
      }

      assertTopKeyAccesses(cache(), "key", 3, false);
      assertTopKeyAccesses(cache(), "key", 1, true);

      //the last put will originate an write skew
      assertLockInformation(cache(), "key", 3, 0, 0);

      assertWriteSkew(cache(), "key", 1);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, true);
      builder.customInterceptors().addInterceptor()
            .before(TxInterceptor.class)
            .interceptor(new CacheUsageInterceptor());
      builder.versioning().enabled(true).scheme(VersioningScheme.SIMPLE);
      builder.transaction().syncCommitPhase(true).syncRollbackPhase(true);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true).lockAcquisitionTimeout(100);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

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
      Long actual;
      if (readAccesses) {
         actual = summaryInterceptor.getLocalTopGets().get(String.valueOf(key));
      } else {
         actual = summaryInterceptor.getLocalTopPuts().get(String.valueOf(key));
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
