package org.infinispan.tx;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.interceptors.DistributionInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

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
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      super.createClusteredCaches(2, config);
      BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(cache(0), cache(1));
   }

   public void testLockCleanup() throws Exception {
      final DelayInterceptor interceptor = new DelayInterceptor();
      advancedCache(0).addInterceptor(interceptor, 1);

      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(1).begin();
               advancedCache(1).lock("k");
               tm(1).suspend();
            } catch (Exception e) {
               log.error(e);
            }
         }
      }, false);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return interceptor.receivedReplRequest;
         }
      });

      TestingUtil.killCacheManagers(manager(1));
      BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(cache(0));

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return interceptor.lockAcquired;
         }
      });

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !TestingUtil.extractLockManager(cache(0)).isLocked("k");
         }
      });
   }


   public class DelayInterceptor extends CommandInterceptor {

      volatile boolean receivedReplRequest = false;

      volatile boolean lockAcquired = false;


      @Override
      public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
         if (!ctx.isOriginLocal()) {
            receivedReplRequest = true;
            Thread.sleep(5000);
            try {
               System.out.println("RemoteLockCleanupTest$DelayInterceptor.visitLockControlCommand");
               return super.visitLockControlCommand(ctx, command);
            } finally {
               lockAcquired = true;
            }
         } else {

            return super.visitLockControlCommand(ctx, command);
         }
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
         try {
            return super.visitRollbackCommand(ctx, command);
         } finally {
            System.out.println("RemoteLockCleanupTest$DelayInterceptor.visitRollbackCommand");
         }
      }
   }
}
