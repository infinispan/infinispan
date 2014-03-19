package org.infinispan.commands.tx.totalorder;

import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The 2nd phase command of 2PC, used when a transaction must be aborted. This implementation is used when Total Order
 * based protocol is used
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderRollbackCommand extends RollbackCommand {

   public static final byte COMMAND_ID = 37;
   private static final Log log = LogFactory.getLog(TotalOrderRollbackCommand.class);

   public TotalOrderRollbackCommand(String cacheName, GlobalTransaction globalTransaction) {
      super(cacheName, globalTransaction);
   }

   public TotalOrderRollbackCommand(String cacheName) {
      super(cacheName);
   }

   private TotalOrderRollbackCommand() {
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
