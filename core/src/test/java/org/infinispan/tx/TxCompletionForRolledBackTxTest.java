package org.infinispan.tx;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertFalse;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "functional", testName = "tx.TxCompletionForRolledBackTxTest")
public class TxCompletionForRolledBackTxTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.clustering().hash().numOwners(1).transaction().lockingMode(LockingMode.PESSIMISTIC);
      amend(dcc);
      createCluster(TestDataSCI.INSTANCE, dcc, 2);
      waitForClusterToForm();
   }

   protected void amend(ConfigurationBuilder dcc) {}

   public void testTxCompletionNotSentForRollback() throws Throwable {
      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever(Mocks.BEFORE_RELEASE);
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);
      ConcurrentMap<Class<?>, AtomicInteger> commandCounters = new ConcurrentHashMap<>();
      Mocks.blockInboundCacheRpcCommand(cache(1), checkPoint, c -> countCommands(c, commandCounters));

      tm(0).begin();
      Object k = getKeyForCache(1);
      cache(0).put(k,"k");
      tm(0).rollback();

      assertNotLocked(k);
      assertNull(cache(0).get(k));

      assertEquals(commandCounters.get(RollbackCommand.class).get(), 1);
      assertFalse(commandCounters.containsKey(TxCompletionNotificationCommand.class));
   }

   public static boolean countCommands(CacheRpcCommand c, ConcurrentMap<Class<?>, AtomicInteger> commandCounters) {
      commandCounters.compute(c.getClass(), (k, v)-> {
         if (v == null) {
            return new AtomicInteger(1);
         } else {
            v.incrementAndGet();
            return v;
         }
      });
      return false;
   }
}
