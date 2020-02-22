package org.infinispan.tx.recovery.admin;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.xa.recovery.RecoveryAdminOperations;
import org.infinispan.transaction.xa.recovery.RecoveryAwareTransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test(groups = "functional")
public abstract class AbstractRecoveryTest extends MultipleCacheManagersTest {

   protected ConfigurationBuilder defaultRecoveryConfig() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .useSynchronization(false)
            .recovery().enable()
            .locking().useLockStriping(false)
            .clustering().hash().numOwners(2)
            .l1().disable()
            .stateTransfer().fetchInMemoryState(false);
      return builder;
   }

   protected int countInDoubtTx(String inDoubt) {
      log.tracef("Retrieved in-doubt transactions: %s", inDoubt);
      int lastIndex = 0;
      int count = 0;
      while ((lastIndex = inDoubt.indexOf("internalId", lastIndex + 1)) >= 0) {
         count ++;
      }
      return count;
   }

   protected RecoveryAdminOperations recoveryOps(int cacheIndex) {
      return advancedCache(cacheIndex).getComponentRegistry().getComponent(RecoveryAdminOperations.class);
   }

   protected List<Long> getInternalIds(String inDoubt) {
      Pattern p = Pattern.compile("internalId = [0-9]*");
      Matcher matcher = p.matcher(inDoubt);
      List<Long> result = new ArrayList<>();
      while (matcher.find()) {
         String group = matcher.group();
         Long id = Long.parseLong(group.substring("internalId = ".length()));
         result.add(id);
      }
      return result;
   }

   protected boolean isSuccess(String result) {
      return result.contains("successful");
   }

   public RecoveryAwareTransactionTable tt(int index) {
      return (RecoveryAwareTransactionTable) advancedCache(index).getComponentRegistry().getComponent(TransactionTable.class);
   }

   protected void checkProperlyCleanup(final int managerIndex) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() {
            return TestingUtil.extractLockManager(cache(managerIndex)).getNumberOfLocksHeld() == 0;
         }
      });
      final TransactionTable tt = TestingUtil.extractComponent(cache(managerIndex), TransactionTable.class);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() {
            log.tracef("For cache %s have remoteTx=%s and localTx=%s", managerIndex, tt.getRemoteTxCount(), tt.getLocalTxCount());
            return (tt.getRemoteTxCount() == 0) && (tt.getLocalTxCount() == 0);
         }
      });
      final RecoveryManager rm = TestingUtil.extractComponent(cache(managerIndex), RecoveryManager.class);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() {
            return rm.getInDoubtTransactions().isEmpty() && rm.getPreparedTransactionsFromCluster().all().length == 0;
         }
      });
   }

   protected void assertCleanup(int... caches) {
      for (int i : caches) {
         checkProperlyCleanup(i);
      }
   }

   protected RecoveryManager recoveryManager(int cacheIndex) {
      return TestingUtil.extractComponent(cache(cacheIndex), RecoveryManager.class);
   }

   protected int getTxParticipant(boolean txParticipant) {
      int expectedNumber = txParticipant ? 1 : 0;

      int index = -1;
      for (int i = 0; i < 2; i++) {
         if (recoveryManager(i).getInDoubtTransactions().size() == expectedNumber) {
            index = i;
            break;
         }
      }
      return index;
   }

}
