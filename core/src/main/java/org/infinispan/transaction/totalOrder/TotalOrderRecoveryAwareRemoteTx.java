package org.infinispan.transaction.totalOrder;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.transaction.TxDependencyLatch;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareRemoteTransaction;

import java.util.Set;

/**
 * Implements the Total Order Remote Transaction interface to be used with the Total Order protocol for Recovery Aware
 * Transactions
 *
 * ps. the javadoc is in the interface
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderRecoveryAwareRemoteTx extends RecoveryAwareRemoteTransaction implements TotalOrderRemoteTransaction {
   private TotalOrderState state;

   public TotalOrderRecoveryAwareRemoteTx(WriteCommand[] modifications, GlobalTransaction tx, int viewId) {
      super(modifications, tx, viewId);
      state = new TotalOrderState(tx);
   }

   public TotalOrderRecoveryAwareRemoteTx(GlobalTransaction tx, int viewId) {
      super(tx, viewId);
      state = new TotalOrderState(tx);
   }

   @Override
   public boolean isMarkedForRollback() {
      return state.isMarkedForRollback();
   }

   @Override
   public boolean isMarkedForCommit() {
      return state.isMarkedForCommit();
   }

   @Override
   public void markPreparedAndNotify() {
      state.markPreparedAndNotify();
   }

   @Override
   public void markForPreparing() {
      state.markForPreparing();
   }

   @Override
   public boolean waitPrepared(boolean commit, EntryVersionsMap newVersions) throws InterruptedException {
      setUpdatedEntryVersions(newVersions);
      return state.waitPrepared(commit);
   }

   @Override
   public Set<Object> getModifiedKeys() {
      return TotalOrderState.getModifiedKeys(getModifications());
   }

   @Override
   public TxDependencyLatch getLatch() {
      return state.getLatch();
   }
}
