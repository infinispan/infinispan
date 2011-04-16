package org.infinispan.transaction.xa.recovery;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;

/**
 * Extends {@link org.infinispan.transaction.RemoteTransaction} and adds recovery related information and functionality.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RecoveryAwareRemoteTransaction extends RemoteTransaction implements RecoveryAwareTransaction {

   private static final Log log = LogFactory.getLog(RecoveryAwareRemoteTransaction.class);

   private boolean prepared;

   private boolean isOrphan;

   public RecoveryAwareRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx) {
      super(modifications, tx);
   }

   public RecoveryAwareRemoteTransaction(GlobalTransaction tx) {
      super(tx);
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
    * @param leavers the nodes that left the cluster
    */
   public void computeOrphan(List<Address> leavers) {
      if (leavers.contains(getGlobalTransaction().getAddress())) {
         if (log.isTraceEnabled()) log.trace("This transaction's originator has left the cluster: %s", getGlobalTransaction());
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
   }

   @Override
   public String toString() {
      return "RecoveryAwareRemoteTransaction{" +
            "prepared=" + prepared +
            ", isOrphan=" + isOrphan +
            ", modifications=" + modifications +
            ", lookedUpEntries=" + lookedUpEntries +
            ", tx=" + tx +
            "} ";
   }
}
