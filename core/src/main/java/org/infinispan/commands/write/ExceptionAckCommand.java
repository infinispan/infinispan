package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.InitializableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.ResponseCollectors;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A command that represents an exception acknowledge sent by any owner.
 * <p>
 * The acknowledge represents an unsuccessful execution of the operation.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class ExceptionAckCommand extends BaseRpcCommand implements InitializableCommand {
   private static final Log log = LogFactory.getLog(ExceptionAckCommand.class);

   public static final byte COMMAND_ID = 42;
   private CommandAckCollector commandAckCollector;
   private Throwable throwable;
   private long id;
   private int topologyId;

   public ExceptionAckCommand() {
      super(null);
   }

   public ExceptionAckCommand(ByteString cacheName) {
      super(cacheName);
   }

   public ExceptionAckCommand(ByteString cacheName, long id, Throwable throwable, int topologyId) {
      super(cacheName);
      this.id = id;
      this.throwable = throwable;
      this.topologyId = topologyId;
   }

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      this.commandAckCollector = componentRegistry.getCommandAckCollector().running();
   }

   public void ack() {
      CacheException remoteException = ResponseCollectors.wrapRemoteException(getOrigin(), this.throwable);
      commandAckCollector.completeExceptionally(id, remoteException, topologyId);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return false;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeLong(id);
      output.writeObject(throwable);
      output.writeInt(topologyId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      id = input.readLong();
      throwable = (Throwable) input.readObject();
      topologyId = input.readInt();
   }

   @Override
   public String toString() {
      return "ExceptionAckCommand{" +
            "id=" + id +
            ", throwable=" + throwable +
            ", topologyId=" + topologyId +
            '}';
   }
}
