package org.infinispan.stats;

import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.interceptors.DDSequentialInterceptor;
import org.infinispan.interceptors.SequentialInterceptorChain;
import org.infinispan.interceptors.impl.TxInterceptor;
import org.infinispan.stats.topK.CacheUsageInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;

import java.util.concurrent.CompletableFuture;

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
      SequentialInterceptorChain interceptorChain =
            cache.getAdvancedCache().getSequentialInterceptorChain();
      return interceptorChain.findInterceptorExtending(CacheUsageInterceptor.class);
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
      SequentialInterceptorChain chain = cache.getAdvancedCache().getSequentialInterceptorChain();

      PrepareCommandBlocker blocker = chain.findInterceptorWithClass(PrepareCommandBlocker.class);
      if (blocker != null)
         return blocker;

      blocker = new PrepareCommandBlocker();
      chain.addInterceptorBefore(blocker, TxInterceptor.class);
      return blocker;
   }

   protected class PrepareCommandBlocker extends DDSequentialInterceptor {

      private boolean unblock = false;
      private boolean prepareBlocked = false;

      @Override
      public CompletableFuture<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
            if (throwable == null) {
               synchronized (this) {
                  prepareBlocked = true;
                  notifyAll();
                  while (!unblock) {
                     wait();
                  }
               }
            }
            return null;
         });
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
