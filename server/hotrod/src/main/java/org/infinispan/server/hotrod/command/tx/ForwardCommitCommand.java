package org.infinispan.server.hotrod.command.tx;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.server.hotrod.command.Ids;
import org.infinispan.server.hotrod.tx.operation.Util;
import org.infinispan.util.ByteString;

/**
 * A {@link CacheRpcCommand} implementation to forward the commit request from a client to the member that run the
 * transaction.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class ForwardCommitCommand extends AbstractForwardTxCommand {

   public ForwardCommitCommand(ByteString cacheName) {
      super(cacheName);
   }

   public ForwardCommitCommand(ByteString cacheName, XidImpl xid, long timeout) {
      super(cacheName, xid, timeout);
   }

   @Override
   public byte getCommandId() {
      return Ids.FORWARD_COMMIT;
   }

   @Override
   public Object invoke() throws Throwable {
      Util.commitLocalTransaction(cache, xid, timeout);
      return null;
   }

   @Override
   public String toString() {
      return "ForwardCommitCommand{" +
            "cacheName=" + cacheName +
            ", xid=" + xid +
            '}';
   }
}
