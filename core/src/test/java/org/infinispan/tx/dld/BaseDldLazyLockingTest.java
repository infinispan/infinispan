package org.infinispan.tx.dld;

import org.infinispan.Cache;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.PerCacheExecutorThread;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import org.infinispan.util.concurrent.locks.LockManager;

import java.util.concurrent.CountDownLatch;

import static org.testng.Assert.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public abstract class BaseDldLazyLockingTest extends BaseDldTest {

   protected void testSymmetricDeadlock(Object k0, Object k1) {

      CountDownLatch replLatch = new CountDownLatch(1);
      rpcManager0.setReplicationLatch(replLatch);
      rpcManager1.setReplicationLatch(replLatch);

      DeadlockDetectingLockManager ddLm0 = (DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(0));
      ddLm0.setExposeJmxStats(true);
      DeadlockDetectingLockManager ddLm1 = (DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(1));
      ddLm1.setExposeJmxStats(true);


      PerCacheExecutorThread t0 = new PerCacheExecutorThread(cache(0), 0);
      PerCacheExecutorThread t1 = new PerCacheExecutorThread(cache(1), 1);

      assert PerCacheExecutorThread.OperationsResult.BEGGIN_TX_OK == t0.execute(PerCacheExecutorThread.Operations.BEGGIN_TX);
      assert PerCacheExecutorThread.OperationsResult.BEGGIN_TX_OK == t1.execute(PerCacheExecutorThread.Operations.BEGGIN_TX);

      t0.setKeyValue(k0, "k0_0");
      t1.setKeyValue(k1, "k1_0");

      t0.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);
      t1.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);
      t0.execute(PerCacheExecutorThread.Operations.FORCE2PC);
      t1.execute(PerCacheExecutorThread.Operations.FORCE2PC);

      t0.setKeyValue(k1, "k1_0");
      t1.setKeyValue(k0, "k0_1");

      assertEquals(t0.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE), PerCacheExecutorThread.OperationsResult.PUT_KEY_VALUE_OK);
      assertEquals(t1.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE), PerCacheExecutorThread.OperationsResult.PUT_KEY_VALUE_OK);

      log.info("---Before commit");
      t0.executeNoResponse(PerCacheExecutorThread.Operations.COMMIT_TX);
      t1.executeNoResponse(PerCacheExecutorThread.Operations.COMMIT_TX);

      replLatch.countDown();


      Object t0Response = t0.waitForResponse();
      Object t1Response = t1.waitForResponse();

      assert xor(t0Response instanceof Exception, t1Response instanceof Exception);

      if (t0Response instanceof Exception) {
         Object o = cache(0).get(k0);
         assert o != null;
         assert o.equals("k0_1");
      } else {
         Object o = cache(1).get(k0);
         assert o != null;
         assert o.equals("k0_0");
      }

      assert ddLm0.getDetectedRemoteDeadlocks() + ddLm1.getDetectedRemoteDeadlocks() >= 1;

      LockManager lm0 = TestingUtil.extractComponent(cache(0), LockManager.class);
      assert !lm0.isLocked("key") : "It is locked by " + lm0.getOwner("key");
      LockManager lm1 = TestingUtil.extractComponent(cache(1), LockManager.class);
      assert !lm1.isLocked("key") : "It is locked by " + lm1.getOwner("key");
   }

   protected void testLocalVsRemoteDeadlock(Object k0, Object k1) {

      PerCacheExecutorThread t0 = new PerCacheExecutorThread(cache(0), 0);
      PerCacheExecutorThread t1 = new PerCacheExecutorThread(cache(1), 1);

      assert PerCacheExecutorThread.OperationsResult.BEGGIN_TX_OK == t0.execute(PerCacheExecutorThread.Operations.BEGGIN_TX);
      assert PerCacheExecutorThread.OperationsResult.BEGGIN_TX_OK == t1.execute(PerCacheExecutorThread.Operations.BEGGIN_TX);

      t0.setKeyValue(k0, "k0_0");
      t0.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);

      t1.setKeyValue(k1, "k1_0");
      assertEquals(t1.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE), PerCacheExecutorThread.OperationsResult.PUT_KEY_VALUE_OK);
      t1.setKeyValue(k0, "k0_1");
      assertEquals(t1.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE), PerCacheExecutorThread.OperationsResult.PUT_KEY_VALUE_OK);
      t1.executeNoResponse(PerCacheExecutorThread.Operations.COMMIT_TX);

      t0.setKeyValue(k1, "k1_0");
      t0.executeNoResponse(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);

      Object t0Response = t0.waitForResponse();
      Object t1Response = t1.waitForResponse();

      boolean v0 = t0Response instanceof Exception;
      boolean v1 = t1Response instanceof Exception;
      assert xor(v0, v1) : "both exceptions? " + (v0 && v1);

      if (!v1) {
         System.out.println("V0" );
         assertEquals(cache(0).get(k0), "k0_1");
         assertEquals(cache(0).get(k1), "k1_1");
         assertEquals(cache(1).get(k0), "k0_1");
         assertEquals(cache(1).get(k1), "k1_1");
      } else {
         System.out.println("v1 = " + v1);
         assertEquals(t0.execute(PerCacheExecutorThread.Operations.COMMIT_TX), PerCacheExecutorThread.OperationsResult.COMMIT_TX_OK);
         assertEquals(cache(0).get(k0), "k0_0");
         assertEquals(cache(0).get(k1), "k1_0");
         assertEquals(cache(1).get(k0), "k0_0");
         assertEquals(cache(1).get(k1), "k1_0");
      }
   }
}
