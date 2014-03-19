package org.infinispan.commands.tx.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.transaction.impl.TotalOrderRemoteTransactionState;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.List;

/**
 * Command corresponding to the 1st phase of 2PC when Total Order based protocol is used. This command is used when non
 * versioned entries are needed.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderNonVersionedPrepareCommand extends PrepareCommand implements TotalOrderPrepareCommand {

   public static final byte COMMAND_ID = 38;

   public TotalOrderNonVersionedPrepareCommand(String cacheName, GlobalTransaction gtx, WriteCommand... modifications) {
      super(cacheName, gtx, true, modifications);
   }

   public TotalOrderNonVersionedPrepareCommand(String cacheName, GlobalTransaction gtx, List<WriteCommand> commands) {
      super(cacheName, gtx, commands, true);
   }

   public TotalOrderNonVersionedPrepareCommand(String cacheName) {
      super(cacheName);
   }

   private TotalOrderNonVersionedPrepareCommand() {
      super(null); // For command id uniqueness test
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void markAsOnePhaseCommit() {
      //no-op. it is always one phase commit
   }

   @Override
   public void markSkipWriteSkewCheck() {
      //no-op. no write skew check in non versioned mode
   }

   @Override
   public boolean skipWriteSkewCheck() {
      return true; //no write skew check with non versioned mode
   }

   @Override
   public TotalOrderRemoteTransactionState getOrCreateState() {
      return getRemoteTransaction().getTransactionState();
   }

}
