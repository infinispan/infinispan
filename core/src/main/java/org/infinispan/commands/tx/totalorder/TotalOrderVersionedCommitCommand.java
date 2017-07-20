package org.infinispan.commands.tx.totalorder;

import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Command corresponding to the 2nd phase of 2PC. Used in Total Order based protocol when versioned entries are needed
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderVersionedCommitCommand extends VersionedCommitCommand {

   public static final byte COMMAND_ID = 36;
   private static final Log log = LogFactory.getLog(TotalOrderVersionedCommitCommand.class);

   public TotalOrderVersionedCommitCommand(ByteString cacheName, GlobalTransaction gtx) {
      super(cacheName, gtx);
   }

   public TotalOrderVersionedCommitCommand(ByteString cacheName) {
      super(cacheName);
   }

   private TotalOrderVersionedCommitCommand() {
      super(null); // For command id uniqueness test
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }
}
