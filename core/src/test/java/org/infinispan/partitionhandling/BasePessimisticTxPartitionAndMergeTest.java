package org.infinispan.partitionhandling;

import org.infinispan.Cache;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

/**
 * It tests multiple scenarios where a split can happen during a transaction.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public abstract class BasePessimisticTxPartitionAndMergeTest extends BaseTxPartitionAndMergeTest {

   protected static final String PESSIMISTIC_TX_CACHE_NAME = "pes-cache";

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().partitionHandling().enabled(true);
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC).transactionMode(TransactionMode.TRANSACTIONAL).transactionManagerLookup(new DummyTransactionManagerLookup());
      defineConfigurationOnAllManagers(PESSIMISTIC_TX_CACHE_NAME, builder);
   }

   protected abstract void checkLocksDuringPartition(SplitMode splitMode, KeyInfo keyInfo, boolean discard);

   protected abstract boolean forceRollback();

   protected abstract Class<? extends TransactionBoundaryCommand> getCommandClass();

   protected void doTest(final SplitMode splitMode, boolean txFail, boolean discard) throws Exception {
      waitForClusterToForm(PESSIMISTIC_TX_CACHE_NAME);

      final KeyInfo keyInfo = createKeys(PESSIMISTIC_TX_CACHE_NAME);
      final Cache<Object, String> originator = cache(0, PESSIMISTIC_TX_CACHE_NAME);
      final FilterCollection filterCollection = createFilters(PESSIMISTIC_TX_CACHE_NAME, discard, getCommandClass(), splitMode);

      Future<Void> put = fork(() -> {
         final TransactionManager transactionManager = originator.getAdvancedCache().getTransactionManager();
         transactionManager.begin();
         final Transaction tx = transactionManager.getTransaction();
         try {
            keyInfo.putFinalValue(originator);
            if (forceRollback()) {
               tx.setRollbackOnly();
            }
         } finally {
            transactionManager.commit();
         }
         return null;
      });

      filterCollection.await(30, TimeUnit.SECONDS);
      splitMode.split(this);
      filterCollection.unblock();

      try {
         put.get();
         assertFalse(txFail);
      } catch (ExecutionException e) {
         assertTrue(txFail);
      }

      checkLocksDuringPartition(splitMode, keyInfo, discard);

      mergeCluster();
      finalAsserts(PESSIMISTIC_TX_CACHE_NAME, keyInfo, txFail ? INITIAL_VALUE : FINAL_VALUE);
   }
}
