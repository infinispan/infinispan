package org.infinispan.tx.dld;

import org.infinispan.config.Configuration;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test (groups = "functional", testName = "tx.dld.SameKeyDeadlockReplicationTest")
public class SameKeyDeadlockReplicationTest extends BaseDldTest {

   Exception e0;
   Exception e1;

   private boolean t1Finished;
   private boolean t2Finished;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getConfiguration();
      config.setUseLockStriping(false);
      config.setEnableDeadlockDetection(true);
      createCluster(config, 2);
      waitForClusterToForm();
      rpcManager0 = replaceRpcManager(cache(0));
      rpcManager1 = replaceRpcManager(cache(1));
   }

   protected Configuration getConfiguration() {
      return getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
   }

   public void testSameKeyDeadlockExplicitLocking() {

      CountDownLatch replicationLatch = new CountDownLatch(1);
      rpcManager0.setReplicationLatch(replicationLatch);
      rpcManager1.setReplicationLatch(replicationLatch);

      log.trace("1 - Before locking");

      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(0).begin();
               advancedCache(0).lock("k");
               tm(0).commit();
            } catch (Exception e) {
               e0 = e;
            } finally {
               t1Finished = true;
            }
         }
      }, false);

      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(1).begin();
               advancedCache(1).lock("k");
               tm(1).commit();
            } catch (Exception e) {
               e1 = e;
            } finally {
               t2Finished = true;
            }
         }
      }, false);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return lockManager(0).isLocked("k") && lockManager(1).isLocked("k");
         }
      });
      log.trace("2 - Both are locked ");

      replicationLatch.countDown();

      log.trace("3 - After countdown ");

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return t1Finished && t2Finished;
         }
      });
      
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            boolean b = xor(e0 instanceof DeadlockDetectedException, e1 instanceof DeadlockDetectedException);
            if (!b) {
               log.error("e0" + e0, e0);
               log.error("e1"+ e1, e1);               
            }
            return b;
         }
      }, 3000);
      assert xor(e0 == null, e1 == null);
   }

}
