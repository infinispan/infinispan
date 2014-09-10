package org.infinispan.transaction.xa.recovery;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Status;

import java.util.Collection;

/**
 * Extends {@link org.infinispan.transaction.impl.RemoteTransaction} and adds recovery related information and functionality.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RecoveryAwareRemoteTransaction extends RemoteTransaction implements RecoveryAwareTransaction {

   private static final Log log = LogFactory.getLog(RecoveryAwareRemoteTransaction.class);

   private boolean prepared;

   private boolean isOrphan;

   private Integer status;

   public RecoveryAwareRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx, int topologyId,
                                         Equivalence<Object> keyEquivalence, long txCreationTime) {
      super(modifications, tx, topologyId, keyEquivalence, txCreationTime);
   }

   public RecoveryAwareRemoteTransaction(GlobalTransaction tx, int topologyId, Equivalence<Object> keyEquivalence,
                                         long txCreationTime) {
      super(tx, topologyId, keyEquivalence, txCreationTime);
   }

   /**
    * A transaction is in doubt if it is prepared and and it is orphan.
    */
   public boolean isInDoubt() {
      return isPrepared() && isOrphan();
   }

   /**
    * A remote transaction is orphan if the node on which the transaction originated (ie the originator) is no longer
    * part of the cluster.
    */
   public boolean isOrphan() {
      return isOrphan;
   }

   /**
    * Check's if this transaction's originator is no longer part of the cluster (orphan transaction) and updates
    * {@link #isOrphan()}.
    * @param currentMembers The current members of the cache.
    */
   public void computeOrphan(Collection<Address> currentMembers) {
      if (!currentMembers.contains(getGlobalTransaction().getAddress())) {
         if (log.isTraceEnabled()) log.tracef("This transaction's originator has left the cluster: %s", getGlobalTransaction());
         isOrphan = true;
      }
   }

   @Override
   public boolean isPrepared() {
      return prepared;
   }

   @Override
   public void setPrepared(boolean prepared) {
      this.prepared = prepared;
      if (prepared) status = Status.STATUS_PREPARED;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() +
            "{prepared=" + prepared +
            ", isOrphan=" + isOrphan +
            ", modifications=" + modifications +
            ", lookedUpEntries=" + lookedUpEntries +
            ", tx=" + tx +
            "} ";
   }

   /**
    * Called when after the 2nd phase of a 2PC is successful.
    *
    * @param committed true if tx successfully committed, false if tx successfully rolled back.
    */
   public void markCompleted(boolean committed) {
      status = committed ? Status.STATUS_COMMITTED : Status.STATUS_ROLLEDBACK;
   }

   /**
    * Following values might be returned:
    * <ul>
    *    <li> - {@link Status#STATUS_PREPARED} if the tx is prepared </li>
    *    <li> - {@link Status#STATUS_COMMITTED} if the tx is committed</li>
    *    <li> - {@link Status#STATUS_ROLLEDBACK} if the tx is rollback</li>
    *    <li> - null otherwise</li>
    * </ul>
    */
   public Integer getStatus() {
      return status;
   }
}
