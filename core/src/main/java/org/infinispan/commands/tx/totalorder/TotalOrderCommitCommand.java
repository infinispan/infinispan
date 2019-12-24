package org.infinispan.commands.tx.totalorder;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;

/**
 * Commit Command used in the 2nd phase of 2PC. This command is used when non versioned entries are needed
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderCommitCommand extends CommitCommand {

   public static final byte COMMAND_ID = 35;

   public TotalOrderCommitCommand(ByteString cacheName, GlobalTransaction gtx) {
      super(cacheName, gtx);
   }

   public TotalOrderCommitCommand(ByteString cacheName) {
      super(cacheName);
   }

   private TotalOrderCommitCommand() {
      super(null); // For command id uniqueness test
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }
}
