package org.infinispan.partitionhandling;

import org.infinispan.Cache;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyTransactionManager;

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
public abstract class BaseOptimisticTxPartitionAndMergeTest extends BaseTxPartitionAndMergeTest {

   protected static final String OPTIMISTIC_TX_CACHE_NAME = "opt-cache";

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().partitionHandling().enabled(true);
      builder.transaction().lockingMode(LockingMode.OPTIMISTIC).transactionMode(TransactionMode.TRANSACTIONAL).transactionManagerLookup(new DummyTransactionManagerLookup());
      defineConfigurationOnAllManagers(OPTIMISTIC_TX_CACHE_NAME, builder);
   }

   protected abstract void checkLocksDuringPartition(SplitMode splitMode, KeyInfo keyInfo, boolean discard);

   protected abstract boolean forceRollback();

   protected abstract Class<? extends TransactionBoundaryCommand> getCommandClass();

   protected void doTest(final SplitMode splitMode, boolean txFail, boolean discard) throws Exception {
      waitForClusterToForm(OPTIMISTIC_TX_CACHE_NAME);

      final KeyInfo keyInfo = createKeys(OPTIMISTIC_TX_CACHE_NAME);
      final Cache<Object, String> originator = cache(0, OPTIMISTIC_TX_CACHE_NAME);
      final FilterCollection filterCollection = createFilters(OPTIMISTIC_TX_CACHE_NAME, discard, getCommandClass(), splitMode);

      Future<Void> put = fork(() -> {
         final DummyTransactionManager transactionManager = (DummyTransactionManager) originator.getAdvancedCache().getTransactionManager();
         transactionManager.begin();
         keyInfo.putFinalValue(originator);
         final DummyTransaction transaction = transactionManager.getTransaction();
         transaction.runPrepare();
         transaction.runCommit(forceRollback());
         transaction.throwRollbackExceptionIfAny();
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
      finalAsserts(OPTIMISTIC_TX_CACHE_NAME, keyInfo, txFail ? INITIAL_VALUE : FINAL_VALUE);
   }
}
