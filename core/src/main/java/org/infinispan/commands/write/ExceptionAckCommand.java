package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commons.CacheException;
import org.infinispan.remoting.transport.ResponseCollectors;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;

/**
 * A command that represents an exception acknowledge sent by any owner.
 * <p>
 * The acknowledge represents an unsuccessful execution of the operation.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class ExceptionAckCommand extends BackupAckCommand {
   public static final byte COMMAND_ID = 42;
   private Throwable throwable;

   public ExceptionAckCommand() {
      super();
   }

   public ExceptionAckCommand(ByteString cacheName) {
      super(cacheName);
   }

   public ExceptionAckCommand(ByteString cacheName, long id, Throwable throwable, int topologyId) {
      super(cacheName, id, topologyId);
      this.throwable = throwable;
   }

   @Override
   public void ack(CommandAckCollector ackCollector) {
      CacheException remoteException = ResponseCollectors.wrapRemoteException(getOrigin(), throwable);
      ackCollector.completeExceptionally(id, remoteException, topologyId);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
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
