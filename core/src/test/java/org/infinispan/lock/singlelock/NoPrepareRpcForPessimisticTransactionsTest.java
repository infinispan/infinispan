package org.infinispan.lock.singlelock;

import static org.testng.Assert.assertEquals;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.inboundhandler.ControllingPerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.CountHandler;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "lock.singlelock.NoPrepareRpcForPessimisticTransactionsTest")
public class NoPrepareRpcForPessimisticTransactionsTest extends MultipleCacheManagersTest {

   private Object k1;

   @Override
   protected void createCacheManagers() throws Throwable {
      final ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      c
         .transaction().lockingMode(LockingMode.PESSIMISTIC)
         .clustering()
            .hash().numOwners(1)
            .l1().disable();
      createCluster(TestDataSCI.INSTANCE, c, 2);
      waitForClusterToForm();

      k1 = getKeyForCache(1);
   }

   public void testSingleGetOnPut() throws Exception {
      runtTest(() -> cache(0).put(k1, "v0"));
   }

   public void testSingleGetOnRemove() throws Exception {
      runtTest(() -> cache(0).remove(k1));
   }

   private void runtTest(Runnable o) throws NotSupportedException, SystemException, RollbackException, HeuristicMixedException, HeuristicRollbackException {
      ControllingPerCacheInboundInvocationHandler handler = ControllingPerCacheInboundInvocationHandler.replace(cache(1));
      CountHandler clusterGetCount = handler.countRpc(ClusteredGetCommand.class);
      CountHandler lockCount = handler.countRpc(LockControlCommand.class);
      CountHandler prepareCount = handler.countRpc(PrepareCommand.class);
      CountHandler txCount = handler.countRpc(TxCompletionNotificationCommand.class);

      clusterGetCount.reset();
      lockCount.reset();
      prepareCount.reset();
      txCount.reset();

      log.trace("Here is where the fun starts..");
      tm(0).begin();

      o.run();

      assertKeyLockedCorrectly(k1);

      assertEquals(1, clusterGetCount.sum());
      assertEquals(1, lockCount.sum());

      tm(0).commit();

      eventually(() -> {
         //prepare + tx completion notification
         return  txCount.sum() == 1 && prepareCount.sum() == 1;
      });
   }
}
