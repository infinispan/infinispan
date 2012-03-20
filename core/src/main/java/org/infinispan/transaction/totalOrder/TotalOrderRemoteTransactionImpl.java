package org.infinispan.transaction.totalOrder;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TxDependencyLatch;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Set;

/**
 * Implements the Total Order Remote Transaction interface to be used with the Total Order protocol
 *
 * ps. the javadoc is in the interface
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderRemoteTransactionImpl extends RemoteTransaction implements TotalOrderRemoteTransaction {

   private TotalOrderState state;

   public TotalOrderRemoteTransactionImpl(WriteCommand[] modifications, GlobalTransaction tx, int viewId) {
      super(modifications, tx, viewId);
      state = new TotalOrderState(tx);
   }

   public TotalOrderRemoteTransactionImpl(GlobalTransaction tx, int viewId) {
      super(tx, viewId);
      state = new TotalOrderState(tx);
   }

   public boolean isMarkedForRollback() {
      return state.isMarkedForRollback();
   }

   public boolean isMarkedForCommit() {
      return state.isMarkedForCommit();
   }

   public void markPreparedAndNotify() {
      state.markPreparedAndNotify();
   }

   public void markForPreparing() {
      state.markForPreparing();
   }

   public boolean waitPrepared(boolean commit, EntryVersionsMap newVersions) throws InterruptedException {
      this.setUpdatedEntryVersions(newVersions);
      return state.waitPrepared(commit);
   }

   public Set<Object> getModifiedKeys() {
      return TotalOrderState.getModifiedKeys(getModifications());
   }

   public TxDependencyLatch getLatch() {
      return state.getLatch();
   }

   @Override
   public String toString() {
      return "TotalOrderRemoteTransaction{" +
            "totalOrderState=" + state +
            ", modifications=" + getModifiedKeys() +
            '}';
   }
}
