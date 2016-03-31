package org.infinispan.stats;

import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.stats.topK.CacheUsageInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;

import java.util.List;

import static java.lang.String.format;
import static org.infinispan.distribution.DistributionTestHelper.addressOf;

/**
 * @author Pedro Ruivo
 * @since 9.0
 */
public abstract class AbstractTopKeyTest extends MultipleCacheManagersTest {

   private static boolean isOwner(Cache<?, ?> cache, Object key) {
      DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
      return dm == null || dm.locate(key).contains(addressOf(cache));
   }

   protected CacheUsageInterceptor getTopKey(Cache<?, ?> cache) {
      for (CommandInterceptor interceptor : cache.getAdvancedCache().getInterceptorChain()) {
         if (interceptor instanceof CacheUsageInterceptor) {
            return (CacheUsageInterceptor) interceptor;
         }
      }
      throw new IllegalStateException();
   }

   protected void assertTopKeyAccesses(Cache<?, ?> cache, String key, long expected, boolean readAccesses) {
      final CacheUsageInterceptor topK = getTopKey(cache);
      final boolean isLocal = isOwner(cache, key);
      eventuallyEquals(format("Wrong number of accesses for key '%s' and cache '%s'.", key, addressOf(cache)),
                       expected,
                       () -> {
                          if (readAccesses) {
                             return (isLocal ? topK.getLocalTopGets() : topK.getRemoteTopGets()).getOrDefault(key, 0L);
                          } else {
                             return (isLocal ? topK.getLocalTopPuts() : topK.getRemoteTopPuts()).getOrDefault(key, 0L);
                          }
                       });
   }

   protected void assertWriteSkew(Cache<?, ?> cache, String key, long expected) {
      final CacheUsageInterceptor topK = getTopKey(cache);
      eventuallyEquals(format("Wrong number of write skew for key '%s' and cache '%s'.", key, addressOf(cache)),
                       expected,
                       () -> topK.getTopWriteSkewFailedKeys().getOrDefault(key, 0L));
   }

   private void assertTopKeyLocked(Cache<?, ?> cache, String key, long expected) {
      final CacheUsageInterceptor topK = getTopKey(cache);
      eventuallyEquals(format("Wrong number of locked key for key '%s' and cache '%s'.", key, addressOf(cache)),
                       expected,
                       () -> topK.getTopLockedKeys().getOrDefault(key, 0L));
   }

   private void assertTopKeyLockContented(Cache<?, ?> cache, String key, long expected) {
      final CacheUsageInterceptor topK = getTopKey(cache);
      eventuallyEquals(format("Wrong number of contented key for key '%s' and cache '%s'.", key, addressOf(cache)),
                       expected,
                       () -> topK.getTopContendedKeys().getOrDefault(key, 0L));
   }

   private void assertTopKeyLockFailed(Cache<?, ?> cache, String key, long expected) {
      final CacheUsageInterceptor topK = getTopKey(cache);
      eventuallyEquals(format("Wrong number of failed locked key for key '%s' and cache '%s'.", key, addressOf(cache)),
                       expected,
                       () -> topK.getTopLockFailedKeys().getOrDefault(key, 0L));
   }

   protected void assertLockInformation(Cache<?, ?> cache, String key, long locked, long contented, long failed) {
      assertTopKeyLocked(cache, key, locked);
      assertTopKeyLockContented(cache, key, contented);
      assertTopKeyLockFailed(cache, key, failed);
   }

   protected PrepareCommandBlocker addPrepareBlockerIfAbsent(Cache<?, ?> cache) {
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

   protected class PrepareCommandBlocker extends CommandInterceptor {

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

      public synchronized void reset() {
         unblock = false;
         prepareBlocked = false;
      }

      public synchronized void unblock() {
         unblock = true;
         notifyAll();
      }

      public synchronized void awaitUntilPrepareBlocked() throws InterruptedException {
         while (!prepareBlocked) {
            wait();
         }
      }
   }

}
