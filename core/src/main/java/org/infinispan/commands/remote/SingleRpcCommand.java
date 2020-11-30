package org.infinispan.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Aggregates a single command for replication.
 *
 * @author Mircea.Markus@jboss.com
 */
public class SingleRpcCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 1;
   private static final Log log = LogFactory.getLog(SingleRpcCommand.class);

   private VisitableCommand command;

   private SingleRpcCommand() {
      super(null); // For command id uniqueness test
   }

   public SingleRpcCommand(ByteString cacheName, VisitableCommand command) {
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
      command = (VisitableCommand) input.readObject();
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      command.init(componentRegistry);
      InvocationContextFactory icf = componentRegistry.getInvocationContextFactory().running();
      InvocationContext ctx = icf.createRemoteInvocationContextForCommand(command, getOrigin());
      if (command instanceof RemoteLockCommand) {
         ctx.setLockOwner(((RemoteLockCommand) command).getKeyLockOwner());
      }
      if (log.isTraceEnabled())
         log.tracef("Invoking command %s, with originLocal flag set to %b", command, ctx
               .isOriginLocal());
      return componentRegistry.getInterceptorChain().running().invokeAsync(ctx, command);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof SingleRpcCommand)) return false;

      SingleRpcCommand that = (SingleRpcCommand) o;
      if (Objects.equals(cacheName, that.cacheName))
         return false;
      return Objects.equals(command, that.command);
   }

   @Override
   public int hashCode() {
      int result = cacheName != null ? cacheName.hashCode() : 0;
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
