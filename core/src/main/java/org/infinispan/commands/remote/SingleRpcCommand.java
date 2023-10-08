package org.infinispan.commands.remote;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.telemetry.InfinispanSpanAttributes;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Aggregates a single command for replication.
 *
 * @author Mircea.Markus@jboss.com
 */
@ProtoTypeId(ProtoStreamTypeIds.SINGLE_RPC_COMMAND)
public class SingleRpcCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 1;
   private static final Log log = LogFactory.getLog(SingleRpcCommand.class);

   final VisitableCommand command;
   private InfinispanSpanAttributes spanAttributes;

   public SingleRpcCommand(ByteString cacheName, VisitableCommand command) {
      super(cacheName);
      this.command = command;
   }

   @ProtoFactory
   SingleRpcCommand(ByteString cacheName, WrappedMessage wrappedCommand) {
      this(cacheName, (VisitableCommand) WrappedMessages.unwrap(wrappedCommand));
   }

   @ProtoField(number = 2, name = "command")
   WrappedMessage getWrappedCommand() {
      return WrappedMessages.orElseNull(command);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
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

   @Override
   public boolean logThrowable(Throwable t) {
      return command.logThrowable(t);
   }

   @Override
   public InfinispanSpanAttributes getSpanAttributes() {
      return spanAttributes;
   }

   @Override
   public String getOperationName() {
      // TODO use the class name or implement this method in all commands?
      return command.getClass().getSimpleName();
   }

   @Override
   public void setSpanAttributes(InfinispanSpanAttributes attributes) {
      spanAttributes = attributes;
   }
}
