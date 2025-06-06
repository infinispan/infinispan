package org.infinispan.tx;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.infinispan.tx.TxCompletionForRolledBackTxTest.countCommands;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertFalse;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "tx.TxCompletionForRolledBackTxOptTest")
public class TxCompletionForRolledBackTxOptTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.clustering().hash().numOwners(1).transaction().lockingMode(LockingMode.OPTIMISTIC);
      createCluster(TestDataSCI.INSTANCE, dcc, 3);
      waitForClusterToForm();
      extractInterceptorChain(advancedCache(2)).addInterceptor(new RollbackBeforePrepareTest.FailPrepareInterceptor(), 1);
   }

   public void testTxCompletionNotSentForRollback() throws Throwable {
      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever(Mocks.BEFORE_RELEASE);
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);
      ConcurrentMap<Class<?>, AtomicInteger> commandCounters = new ConcurrentHashMap<>();
      Mocks.blockInboundCacheRpcCommand(cache(1), checkPoint, c -> countCommands(c, commandCounters));

      tm(0).begin();
      Object k1 = getKeyForCache(1);
      Object k2 = getKeyForCache(2);
      cache(0).put(k1, k1);
      cache(0).put(k2, k2);
      try {
         tm(0).commit();
         fail();
      } catch (Throwable t) {
         log.debugf("Got expected exception", t);
      }

      assertNotLocked(k1);
      assertNotLocked(k2);
      assertNull(cache(0).get(k1));
      assertNull(cache(0).get(k2));

      assertEquals(commandCounters.get(VersionedPrepareCommand.class).get(), 1);
      assertEquals(commandCounters.get(RollbackCommand.class).get(), 1);
      assertFalse(commandCounters.containsKey(TxCompletionNotificationCommand.class));
   }
}
