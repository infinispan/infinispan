package org.infinispan.server.hotrod.command.tx;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.util.ByteString;

/**
 * Abstract class that provides common methods for {@link ForwardCommitCommand} and {@link ForwardRollbackCommand}.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
abstract class AbstractForwardTxCommand extends BaseRpcCommand {

   @ProtoField(2)
   protected XidImpl xid;

   @ProtoField(3)
   protected long timeout;

   AbstractForwardTxCommand(ByteString cacheName) {
      super(cacheName);
   }

   @ProtoFactory
   AbstractForwardTxCommand(ByteString cacheName, XidImpl xid, long timeout) {
      super(cacheName);
      this.xid = xid;
      this.timeout = timeout;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }
}
