package org.infinispan.server.hotrod.command.tx;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.InitializableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;

/**
 * Abstract class that provides common methods for {@link ForwardCommitCommand} and {@link ForwardRollbackCommand}.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
abstract class AbstractForwardTxCommand extends BaseRpcCommand implements InitializableCommand {

   protected XidImpl xid;
   protected long timeout;
   protected AdvancedCache cache;

   AbstractForwardTxCommand(ByteString cacheName) {
      super(cacheName);
   }

   AbstractForwardTxCommand(ByteString cacheName, XidImpl xid, long timeout) {
      super(cacheName);
      this.xid = xid;
      this.timeout = timeout;
   }

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      this.cache = componentRegistry.getCache().wired();
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      XidImpl.writeTo(output, xid);
      output.writeLong(timeout);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException {
      xid = XidImpl.readFrom(input);
      timeout = input.readLong();
   }
}
