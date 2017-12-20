package org.infinispan.partitionhandling;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;

/**
 * It tests multiple scenarios where a split can happen during a transaction.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public abstract class BasePessimisticTxPartitionAndMergeTest extends BaseTxPartitionAndMergeTest {

   static final String PESSIMISTIC_TX_CACHE_NAME = "pes-cache";

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES);
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC).transactionMode(TransactionMode.TRANSACTIONAL).transactionManagerLookup(new EmbeddedTransactionManagerLookup());
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
      filterCollection.stopDiscard();

      mergeCluster(PESSIMISTIC_TX_CACHE_NAME);
      finalAsserts(PESSIMISTIC_TX_CACHE_NAME, keyInfo, txFail ? INITIAL_VALUE : FINAL_VALUE);
   }
}
