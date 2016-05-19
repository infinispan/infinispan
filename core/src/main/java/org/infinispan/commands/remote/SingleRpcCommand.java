package org.infinispan.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.ByteString;

/**
 * Aggregates a single command for replication.
 *
 * @author Mircea.Markus@jboss.com
 */
public class SingleRpcCommand extends BaseRpcInvokingCommand {
   public static final int COMMAND_ID = 1;

   private ReplicableCommand command;

   private SingleRpcCommand() {
      super(null); // For command id uniqueness test
   }

   public SingleRpcCommand(ByteString cacheName, ReplicableCommand command) {
      super(cacheName);
      this.command = command;
   }

   public SingleRpcCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(command);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      command = (ReplicableCommand) input.readObject();
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      return processVisitableCommandAsync(command);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof SingleRpcCommand)) return false;

      SingleRpcCommand that = (SingleRpcCommand) o;

      if (cacheName != null ? !cacheName.equals(that.cacheName) : that.cacheName != null) return false;
      if (command != null ? !command.equals(that.command) : that.command != null) return false;
      if (interceptorChain != null ? !interceptorChain.equals(that.interceptorChain) : that.interceptorChain != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = interceptorChain != null ? interceptorChain.hashCode() : 0;
      result = 31 * result + (cacheName != null ? cacheName.hashCode() : 0);
      result = 31 * result + (command != null ? command.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "SingleRpcCommand{" +
            "cacheName='" + cacheName + '\'' +
            ", command=" + command +
            '}';
   }

   public ReplicableCommand getCommand() {
      return command;
   }

   @Override
   public boolean isReturnValueExpected() {
      return command.isReturnValueExpected();
   }

   @Override
   public boolean isSuccessful() {
      return command.isSuccessful();
   }

   @Override
   public boolean canBlock() {
      return command.canBlock();
   }
}
