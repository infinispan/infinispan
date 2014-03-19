package org.infinispan.commands.tx.totalorder;

import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.transaction.impl.TotalOrderRemoteTransactionState;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.List;

/**
 * Command corresponding to the 1st phase of 2PC when Total Order based protocol is used. This command is used when
 * versioned entries are needed.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderVersionedPrepareCommand extends VersionedPrepareCommand implements TotalOrderPrepareCommand {

   public static final byte COMMAND_ID = 39;
   private boolean skipWriteSkewCheck;

   public TotalOrderVersionedPrepareCommand(String cacheName, GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhase) {
      super(cacheName, gtx, modifications, onePhase);
   }

   public TotalOrderVersionedPrepareCommand(String cacheName) {
      super(cacheName);
   }

   private TotalOrderVersionedPrepareCommand() {
      super(null); // For command id uniqueness test
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void markAsOnePhaseCommit() {
      this.onePhaseCommit = true;
   }

   @Override
   public void markSkipWriteSkewCheck() {
      this.skipWriteSkewCheck = true;
   }

   @Override
   public boolean skipWriteSkewCheck() {
      return skipWriteSkewCheck;
   }

   @Override
   public TotalOrderRemoteTransactionState getOrCreateState() {
      return getRemoteTransaction().getTransactionState();
   }

}
