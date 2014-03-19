package org.infinispan.commands.tx.totalorder;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Commit Command used in the 2nd phase of 2PC. This command is used when non versioned entries are needed
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderCommitCommand extends CommitCommand {

   public static final byte COMMAND_ID = 35;
   private static final Log log = LogFactory.getLog(TotalOrderCommitCommand.class);

   public TotalOrderCommitCommand(String cacheName, GlobalTransaction gtx) {
      super(cacheName, gtx);
   }

   public TotalOrderCommitCommand(String cacheName) {
      super(cacheName);
   }

   private TotalOrderCommitCommand() {
      super(null); // For command id uniqueness test
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   protected RemoteTransaction getRemoteTransaction() {
      return txTable.getOrCreateRemoteTransaction(globalTx, null);
   }
}
