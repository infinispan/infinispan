package org.infinispan.commands.tx.totalorder;

import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;

/**
 * The 2nd phase command of 2PC, used when a transaction must be aborted. This implementation is used when Total Order
 * based protocol is used
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderRollbackCommand extends RollbackCommand {

   public static final byte COMMAND_ID = 37;

   public TotalOrderRollbackCommand(ByteString cacheName, GlobalTransaction globalTransaction) {
      super(cacheName, globalTransaction);
   }

   public TotalOrderRollbackCommand(ByteString cacheName) {
      super(cacheName);
   }

   private TotalOrderRollbackCommand() {
      super(null); // For command id uniqueness test
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }
}
