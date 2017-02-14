package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * A command that represents an exception acknowledge sent by any owner.
 * <p>
 * The acknowledge represents an unsuccessful execution of the operation.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class ExceptionAckCommand extends BaseRpcCommand {

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
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      commandAckCollector.completeExceptionally(id, throwable, topologyId);
      return CompletableFutures.completedNull();
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

   public void setCommandAckCollector(CommandAckCollector commandAckCollector) {
      this.commandAckCollector = commandAckCollector;
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
