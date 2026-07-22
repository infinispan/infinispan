package org.infinispan.tx;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Collections;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.testng.annotations.Test;

/**
 * Verifies that a {@link LockControlCommand} arriving after a transaction has been completed
 * (committed or rolled back) does not acquire locks or create a {@link org.infinispan.transaction.impl.RemoteTransaction}.
 *
 */
@Test(groups = "functional", testName = "tx.DelayedLockControlCommandTest")
public class DelayedLockControlCommandTest extends MultipleCacheManagersTest {

   private Object k;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      builder.transaction()
            .lockingMode(LockingMode.PESSIMISTIC)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      createCluster(TestDataSCI.INSTANCE, builder, 2);
      waitForClusterToForm();
      k = new MagicKey("k", cache(0));
   }

   public void testAfterCommit() throws Throwable {
      TransactionTable txTable0 = ComponentRegistry.of(cache(0)).getTransactionTable();
      TransactionTable txTable1 = ComponentRegistry.of(cache(1)).getTransactionTable();

      tm(0).begin();
      cache(0).put(k, "v1");
      GlobalTransaction gtx = txTable0.getGlobalTransaction(tm(0).getTransaction());
      tm(0).commit();

      assertNoTransactions();
      eventually(() -> txTable1.isTransactionCompleted(gtx));

      invokeDelayedLockControlCommand(txTable1, gtx);
   }

   public void testAfterRollback() throws Throwable {
      TransactionTable txTable0 = ComponentRegistry.of(cache(0)).getTransactionTable();
      TransactionTable txTable1 = ComponentRegistry.of(cache(1)).getTransactionTable();

      tm(0).begin();
      cache(0).put(k, "v1");
      GlobalTransaction gtx = txTable0.getGlobalTransaction(tm(0).getTransaction());
      tm(0).rollback();

      assertNoTransactions();
      eventually(() -> txTable1.isTransactionCompleted(gtx));

      invokeDelayedLockControlCommand(txTable1, gtx);
   }

   private void invokeDelayedLockControlCommand(TransactionTable txTable, GlobalTransaction gtx) throws Throwable {
      assertNull(txTable.getOrCreateRemoteTransaction(gtx, Collections.emptyList()));

      CommandsFactory cf = TestingUtil.extractCommandsFactory(cache(1));
      LockControlCommand lcc = cf.buildLockControlCommand(Collections.singletonList(k), 0, gtx);
      ComponentRegistry registry = ComponentRegistry.of(cache(1));
      Object result = CompletionStages.join(lcc.invokeAsync(registry));

      assertNull(result);
      assertFalse(TestingUtil.extractLockManager(cache(1)).isLocked(k));
      assertNull(txTable.getRemoteTransaction(gtx));
   }
}
