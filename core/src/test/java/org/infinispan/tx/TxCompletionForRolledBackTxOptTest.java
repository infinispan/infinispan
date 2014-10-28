package org.infinispan.tx;

import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.mocks.ControlledCommandFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

@Test(groups = "functional", testName = "tx.TxCompletionForRolledBackTxOptTest")
public class TxCompletionForRolledBackTxOptTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.clustering().hash().numOwners(1).transaction().lockingMode(LockingMode.OPTIMISTIC);
      createCluster(dcc, 3);
      waitForClusterToForm();
      advancedCache(2).addInterceptor(new RollbackBeforePrepareTest.FailPrepareInterceptor(), 1);
   }

   public void testTxCompletionNotSentForRollback() throws Throwable {
      ControlledCommandFactory cf = ControlledCommandFactory.registerControlledCommandFactory(cache(1), null);

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

      assertEquals(cf.received(PrepareCommand.class), 1);
      assertEquals(cf.received(RollbackCommand.class), 1);
      assertEquals(cf.received(TxCompletionNotificationCommand.class), 0);
   }
}
