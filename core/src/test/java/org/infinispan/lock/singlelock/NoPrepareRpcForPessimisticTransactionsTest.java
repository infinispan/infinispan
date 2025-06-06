package org.infinispan.lock.singlelock;

import static org.infinispan.tx.TxCompletionForRolledBackTxTest.countCommands;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.transaction.LockingMode;
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
   private final ConcurrentMap<Class<?>, AtomicInteger> commandCounters = new ConcurrentHashMap<>();

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
      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);
      checkPoint.triggerForever(Mocks.BEFORE_RELEASE);
      Mocks.blockInboundCacheRpcCommand(cache(1), checkPoint, c -> countCommands(c, commandCounters));
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
      commandCounters.clear();
      tm(0).begin();
      o.execute();
      assertKeyLockedCorrectly(k1);
      assertEquals("2 = cluster get + lock", 2, commandCounters.size());

      tm(0).commit();

      eventually(() -> {
         //prepare + tx completion notification
         return commandCounters.size() == 4;
      });
   }

   private interface Operation {
      void execute();
   }
}
