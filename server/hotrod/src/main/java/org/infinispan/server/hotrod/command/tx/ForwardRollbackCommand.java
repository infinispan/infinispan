package org.infinispan.server.hotrod.command.tx;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.server.hotrod.command.Ids;
import org.infinispan.server.hotrod.tx.operation.Util;
import org.infinispan.util.ByteString;

/**
 * A {@link CacheRpcCommand} implementation to forward the rollback request from a client to the member that run the
 * transaction.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class ForwardRollbackCommand extends AbstractForwardTxCommand {


   public ForwardRollbackCommand(ByteString cacheName) {
      super(cacheName);
   }

   public ForwardRollbackCommand(ByteString cacheName, XidImpl xid, long timeout) {
      super(cacheName, xid, timeout);
   }

   @Override
   public byte getCommandId() {
      return Ids.FORWARD_ROLLBACK;
   }

   @Override
   public Object invoke() throws Throwable {
      Util.rollbackLocalTransaction(cache, xid, timeout);
      return null;
   }

   @Override
   public String toString() {
      return "ForwardRollbackCommand{" +
            "cacheName=" + cacheName +
            ", xid=" + xid +
            '}';
   }
}
