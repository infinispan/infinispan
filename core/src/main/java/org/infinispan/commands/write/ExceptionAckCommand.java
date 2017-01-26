package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.ReplicableCommand;
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
public class ExceptionAckCommand implements ReplicableCommand {

   public static final byte COMMAND_ID = 42;
   private CommandAckCollector commandAckCollector;
   private Throwable throwable;
   private long id;
   private int topologyId;

   public ExceptionAckCommand() {
      super();
   }

   public ExceptionAckCommand(long id, Throwable throwable, int topologyId) {
      this.id = id;
      this.throwable = throwable;
      this.topologyId = topologyId;
   }

   public ExceptionAckCommand(CommandAckCollector commandAckCollector) {
      this.commandAckCollector = commandAckCollector;
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

   @Override
   public String toString() {
      return "ExceptionAckCommand{" +
            "id=" + id +
            ", throwable=" + throwable +
            ", topologyId=" + topologyId +
            '}';
   }
}
