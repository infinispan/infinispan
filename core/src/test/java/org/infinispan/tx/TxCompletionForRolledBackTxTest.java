package org.infinispan.tx;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.mocks.ControlledCommandFactory;
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
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   protected void amend(ConfigurationBuilder dcc) {}

   public void testTxCompletionNotSentForRollback() throws Throwable {
      ControlledCommandFactory cf = ControlledCommandFactory.registerControlledCommandFactory(cache(1), null);

      tm(0).begin();
      Object k = getKeyForCache(1);
      cache(0).put(k,"k");
      tm(0).rollback();

      assertNotLocked(k);
      assertNull(cache(0).get(k));

      assertEquals(cf.received(RollbackCommand.class), 1);
      assertEquals(cf.received(TxCompletionNotificationCommand.class), 0);
   }
}
