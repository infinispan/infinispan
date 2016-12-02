package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * A command sent from the primary owner to the backup owners of a key with the new update.
 * <p>
 * This command is only visited by the backups owner and in a remote context. No locks are acquired since it is sent in
 * FIFO order. It can represent a update or remove operation.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class BackupWriteRcpCommand extends BaseRpcCommand implements TopologyAffectedCommand {

   public static final byte COMMAND_ID = 61;
   private static final Operation[] CACHED_VALUES = Operation.values();

   private Operation operation;
   private CommandInvocationId commandInvocationId;
   private Object key;
   private Object value;
   private Metadata metadata;
   private int topologyId;
   private long flags;

   private InvocationContextFactory invocationContextFactory;
   private AsyncInterceptorChain interceptorChain;
   private CacheNotifier cacheNotifier;

   //for org.infinispan.commands.CommandIdUniquenessTest
   public BackupWriteRcpCommand() {
      super(null);
   }

   public BackupWriteRcpCommand(ByteString cacheName) {
      super(cacheName);
   }

   private static Operation valueOf(int index) {
      return CACHED_VALUES[index];
   }

   public void setWrite(CommandInvocationId id, Object key, Object value, Metadata metadata, long flags,
         int topologyId) {
      this.operation = Operation.WRITE;
      setCommonAttributes(id, key, flags, topologyId);
      this.value = value;
      this.metadata = metadata;
   }

   public void setRemove(CommandInvocationId id, Object key, long flags, int topologyId) {
      this.operation = Operation.REMOVE;
      setCommonAttributes(id, key, flags, topologyId);
   }

   public void setRemoveExpired(CommandInvocationId id, Object key, Object value, long flags, int topologyId) {
      this.operation = Operation.REMOVE_EXPIRED;
      setCommonAttributes(id, key, flags, topologyId);
      this.value = value;
   }

   public void setReplace(CommandInvocationId id, Object key, Object value, Metadata metadata, long flags,
         int topologyId) {
      this.operation = Operation.REPLACE;
      setCommonAttributes(id, key, flags, topologyId);
      this.value = value;
      this.metadata = metadata;
   }

   public void init(InvocationContextFactory invocationContextFactory, AsyncInterceptorChain interceptorChain,
         CacheNotifier cacheNotifier) {
      this.invocationContextFactory = invocationContextFactory;
      this.interceptorChain = interceptorChain;
      this.cacheNotifier = cacheNotifier;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      DataWriteCommand command;
      switch (operation) {
         case REMOVE:
            command = new RemoveCommand(key, null, cacheNotifier, flags, null, commandInvocationId);
            break;
         case REMOVE_EXPIRED:
            command = new RemoveExpiredCommand(key, value, null, cacheNotifier, null, commandInvocationId);
            break;
         case WRITE:
            command = new PutKeyValueCommand(key, value, false, cacheNotifier, metadata, flags, null,
                  commandInvocationId);
            break;
         case REPLACE:
            command = new ReplaceCommand(key, null, value, cacheNotifier, metadata, flags, null, commandInvocationId);
            break;
         default:
            throw new IllegalStateException();
      }
      command.addFlag(Flag.SKIP_LOCKING);
      command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
      command.setTopologyId(topologyId);
      InvocationContext invocationContext = invocationContextFactory
            .createRemoteInvocationContextForCommand(command, getOrigin());
      interceptorChain.invokeAsync(invocationContext, command);
      return CompletableFutures.completedNull();
   }

   public Object getKey() {
      return key;
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
      return true;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallEnum(operation, output);
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeObject(key);
      switch (operation) {
         case WRITE:
         case REPLACE:
            output.writeObject(value);
            output.writeObject(metadata);
            break;
         case REMOVE_EXPIRED:
            output.writeObject(value);
            break;
         default:
      }
      output.writeLong(Flag.copyWithoutRemotableFlags(flags));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      operation = MarshallUtil.unmarshallEnum(input, BackupWriteRcpCommand::valueOf);
      commandInvocationId = CommandInvocationId.readFrom(input);
      key = input.readObject();
      switch (operation) {
         case WRITE:
         case REPLACE:
            value = input.readObject();
            metadata = (Metadata) input.readObject();
            break;
         case REMOVE_EXPIRED:
            value = input.readObject();
            break;
         default:
      }
      this.flags = input.readLong();
   }

   @Override
   public String toString() {
      return "BackupWriteRcpCommand{" +
            "operation=" + operation +
            ", commandInvocationId=" + commandInvocationId +
            ", key=" + key +
            ", value=" + value +
            ", metadata=" + metadata +
            ", topologyId=" + topologyId +
            ", flags=" + EnumUtil.prettyPrintBitSet(flags, Flag.class) +
            '}';
   }

   public boolean isRemove() {
      return operation == Operation.REMOVE || operation == Operation.REMOVE_EXPIRED;
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   public Object getValue() {
      return value;
   }

   private void setCommonAttributes(CommandInvocationId commandInvocationId, Object key, long flags, int topologyId) {
      this.commandInvocationId = commandInvocationId;
      this.key = key;
      this.flags = flags;
      this.topologyId = topologyId;
   }

   private enum Operation {
      WRITE,
      REMOVE,
      REMOVE_EXPIRED,
      REPLACE
   }
}
