package org.infinispan.lock.singlelock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "lock.singlelock.NoPrepareRpcForPessimisticTransactionsTest")
public class NoPrepareRpcForPessimisticTransactionsTest extends MultipleCacheManagersTest {

   private Object k1;
   private PerCacheInboundInvocationHandler spy;

   @Override
   protected void createCacheManagers() throws Throwable {
      final ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cb
            .transaction().lockingMode(LockingMode.PESSIMISTIC)
            .clustering()
            .hash().numOwners(1)
            .l1().disable();
      createCluster(TestDataSCI.INSTANCE, cb, 2);
      waitForClusterToForm();
      k1 = getKeyForCache(1);
      spy = TestingUtil.wrapInboundInvocationHandler(cache(1), Mockito::spy);
   }

   public void testSingleGetOnPut() throws Exception {
      Operation o = () -> cache(0).put(k1, "v0");
      runtTest(o);
   }

   public void testSingleGetOnRemove() throws Exception {
      Operation o = () -> cache(0).remove(k1);
      runtTest(o);
   }

   private void runtTest(Operation o) throws NotSupportedException, SystemException, RollbackException, HeuristicMixedException, HeuristicRollbackException {
      Mockito.reset(spy);
      tm(0).begin();
      o.execute();
      assertKeyLockedCorrectly(k1);
      verify(spy).handle(Mockito.any(ClusteredGetCommand.class), any(), any());
      verify(spy).handle(Mockito.any(LockControlCommand.class), any(), any());

      tm(0).commit();

      verify(spy, Mockito.timeout(1_000)).handle(any(PrepareCommand.class), any(), any());
      verify(spy, Mockito.timeout(1_000)).handle(any(TxCompletionNotificationCommand.class), any(), any());
   }

   private interface Operation {
      void execute();
   }
}
