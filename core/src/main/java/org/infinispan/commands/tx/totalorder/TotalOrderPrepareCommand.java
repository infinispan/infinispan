package org.infinispan.commands.tx.totalorder;

import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.transaction.impl.TotalOrderRemoteTransactionState;

/**
 * Interface with the utilities methods that the prepare command must have when Total Order based protocol is used
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public interface TotalOrderPrepareCommand extends TransactionBoundaryCommand {

   /**
    * marks the prepare phase as 1PC to apply immediately the modifications. It is used when the {@code
    * org.infinispan.commands.tx.CommitCommand} is received before the {@code org.infinispan.commands.tx.PrepareCommand}.
    */
   void markAsOnePhaseCommit();

   /**
    * it signals that the write skew check is not needed (for versioned entries). It is used when the {@code
    * org.infinispan.commands.tx.CommitCommand} is received before the {@code org.infinispan.commands.tx.PrepareCommand}.
    */
   void markSkipWriteSkewCheck();

   /**
    * @return {@code true} when the write skew check is not needed.
    */
   boolean skipWriteSkewCheck();

   /**
    * @return the modifications performed by this transaction
    */
   WriteCommand[] getModifications();

   /**
    * returns the {@link TotalOrderRemoteTransactionState} associated with this transaction, creating one if no one was
    * associated to this transaction.
    *
    * @return returns the {@link TotalOrderRemoteTransactionState} associated with this transaction.
    */
   TotalOrderRemoteTransactionState getOrCreateState();
}
