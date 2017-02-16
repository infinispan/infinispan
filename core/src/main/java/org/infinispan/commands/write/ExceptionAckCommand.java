package org.infinispan.commands.write;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.CompletableFutures;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

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
   private CommandInvocationId commandInvocationId;
   private CommandAckCollector commandAckCollector;
   private Throwable throwable;
   private int topologyId;

   public ExceptionAckCommand() {
      super(null);
   }

   public ExceptionAckCommand(ByteString cacheName) {
      super(cacheName);
   }

   public ExceptionAckCommand(ByteString cacheName, CommandInvocationId commandInvocationId, Throwable throwable, int topologyId) {
      super(cacheName);
      this.commandInvocationId = commandInvocationId;
      this.throwable = throwable;
      this.topologyId = topologyId;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      commandAckCollector.completeExceptionally(commandInvocationId, throwable, topologyId);
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
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeObject(throwable);
      output.writeInt(topologyId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      throwable = (Throwable) input.readObject();
      topologyId = input.readInt();
   }

   public void setCommandAckCollector(CommandAckCollector commandAckCollector) {
      this.commandAckCollector = commandAckCollector;
   }

   @Override
   public String toString() {
      return "ExceptionAckCommand{" +
            "commandInvocationId=" + commandInvocationId +
            ", throwable=" + throwable +
            ", topologyId=" + topologyId +
            '}';
   }
}
