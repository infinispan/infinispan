package org.infinispan.tx;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test fpr ISPN-777.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "tx.RemoteLockCleanupTest")
public class RemoteLockCleanupTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      config.transaction().lockingMode(LockingMode.PESSIMISTIC);
      super.createClusteredCaches(2, config);
   }

   public void testLockCleanup() throws Exception {
      final DelayInterceptor interceptor = new DelayInterceptor();
      advancedCache(0).addInterceptor(interceptor, 1);
      final Object k = getKeyForCache(0);

      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(1).begin();
               advancedCache(1).lock(k);
               tm(1).suspend();
            } catch (Exception e) {
               log.error(e);
            }
         }
      });

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return interceptor.receivedReplRequest.get();
         }
      });

      TestingUtil.killCacheManagers(manager(1));
      TestingUtil.blockUntilViewsReceived(60000, false, cache(0));
      TestingUtil.waitForRehashToComplete(cache(0));

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return interceptor.lockAcquired.get();
         }
      });

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !TestingUtil.extractLockManager(cache(0)).isLocked("k");
         }
      });
   }


   static public class DelayInterceptor extends CommandInterceptor {

      AtomicBoolean receivedReplRequest = new AtomicBoolean();
      AtomicBoolean lockAcquired = new AtomicBoolean();

      @Override
      public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
         if (!ctx.isOriginLocal()) {
            receivedReplRequest.set(true);
            // TODO: we can replace this with the BlockingInterceptor instead or something equivalent to remove 5s wait
            Thread.sleep(5000);
            try {
               return super.visitLockControlCommand(ctx, command);
            } finally {
               lockAcquired.set(true);
            }
         } else {

            return super.visitLockControlCommand(ctx, command);
         }
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
         return super.visitRollbackCommand(ctx, command);
      }
   }
}
