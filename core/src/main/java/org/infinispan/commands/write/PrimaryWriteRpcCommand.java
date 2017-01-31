package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class PrimaryWriteRpcCommand extends BaseRpcCommand implements TopologyAffectedCommand, RemoteLockCommand {

   public static final byte COMMAND_ID = 67;
   private static final Operation[] CACHED_VALUES = Operation.values();

   private Operation operation;
   private CommandInvocationId commandInvocationId;
   private Object key;
   private Object value;
   private Metadata metadata;
   private Object conditionalValue;
   private int topologyId;
   private long flags;
   private ValueMatcher valueMatcher;
   private Long removeExpiredLifespan;
   private CacheNotifier notifier;
   private AsyncInterceptorChain interceptorChain;
   private InvocationContextFactory invocationContextFactory;

   public PrimaryWriteRpcCommand() {
      super(null);
   }

   public PrimaryWriteRpcCommand(ByteString cacheName) {
      super(cacheName);
   }

   public void init(CacheNotifier cacheNotifier, AsyncInterceptorChain interceptorChain,
         InvocationContextFactory invocationContextFactory) {
      this.notifier = cacheNotifier;
      this.interceptorChain = interceptorChain;
      this.invocationContextFactory = invocationContextFactory;
   }

   public void initWithDataWriteCommand(DataWriteCommand command) {
      initDataWriteCommand(command);
      switch (command.getCommandId()) {
         case PutKeyValueCommand.COMMAND_ID:
            initWithPutCommand((PutKeyValueCommand) command);
            break;
         case ReplaceCommand.COMMAND_ID:
            initWithReplaceCommand((ReplaceCommand) command);
            break;
         case RemoveCommand.COMMAND_ID:
            initWithRemoveCommand((RemoveCommand) command);
            break;
         case RemoveExpiredCommand.COMMAND_ID:
            initWithRemoveExpiredCommand((RemoveExpiredCommand) command);
            break;
         default:
            throw new IllegalStateException(String.valueOf(command));
      }
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false; //only used by triangle so far and it uses acks to communicate
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      DataWriteCommand command;
      switch (operation) {
         case CONDITIONAL_REMOVE:
         case REMOVE:
            command = new RemoveCommand(key, conditionalValue, notifier, flags, commandInvocationId);
            break;
         case CONDITIONAL_REPLACE:
         case REPLACE:
            command = new ReplaceCommand(key, conditionalValue, value, notifier, metadata, flags, commandInvocationId);
            break;
         case PUT:
            command = new PutKeyValueCommand(key, value, false, notifier, metadata, flags, commandInvocationId);
            break;
         case PUT_IF_ABSENT:
            command = new PutKeyValueCommand(key, value, true, notifier, metadata, flags, commandInvocationId);
            break;
         case REMOVE_EXPIRED:
            command = new RemoveExpiredCommand(key, conditionalValue, removeExpiredLifespan, notifier, commandInvocationId);
            break;
         default:
            throw new IllegalStateException();

      }
      command.setValueMatcher(valueMatcher);
      command.setTopologyId(topologyId);
      InvocationContext invocationContext = invocationContextFactory
            .createRemoteInvocationContextForCommand(command, getOrigin());
      invocationContext.setLockOwner(commandInvocationId);
      return interceptorChain.invokeAsync(invocationContext, command);
   }

   @Override
   public String toString() {
      return "PrimaryWriteRpcCommand{" +
            "operation=" + operation +
            ", commandInvocationId=" + commandInvocationId +
            ", key=" + key +
            ", value=" + value +
            ", metadata=" + metadata +
            ", conditionalValue=" + conditionalValue +
            ", topologyId=" + topologyId +
            ", flags=" + flags +
            ", valueMatcher=" + valueMatcher +
            '}';
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallEnum(operation, output);
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(flags));
      MarshallUtil.marshallEnum(valueMatcher, output);
      output.writeObject(key);
      writeValue(output);
      writeConditionalValue(output);
      writeLifespan(output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      operation = MarshallUtil.unmarshallEnum(input, ordinal -> CACHED_VALUES[ordinal]);
      commandInvocationId = CommandInvocationId.readFrom(input);
      flags = input.readLong();
      valueMatcher = MarshallUtil.unmarshallEnum(input, ValueMatcher::valueOf);
      key = input.readObject();
      readValue(input);
      readConditionalValue(input);
      readLifespan(input);
   }

   @Override
   public Collection<?> getKeysToLock() {
      return Collections.singletonList(key);
   }

   @Override
   public Object getKeyLockOwner() {
      return commandInvocationId;
   }

   @Override
   public boolean hasZeroLockAcquisition() {
      return EnumUtil.containsAny(flags, FlagBitSets.ZERO_LOCK_ACQUISITION_TIMEOUT);
   }

   @Override
   public boolean hasSkipLocking() {
      return EnumUtil.containsAny(flags, FlagBitSets.SKIP_LOCKING);
   }

   private void writeLifespan(ObjectOutput output) throws IOException {
      if (operation == Operation.REMOVE_EXPIRED) {
         boolean hasLifespan = removeExpiredLifespan != null;
         output.writeBoolean(hasLifespan);
         output.writeLong(removeExpiredLifespan);
      }
   }

   private void readLifespan(ObjectInput input) throws IOException {
      if (operation == Operation.REMOVE_EXPIRED && input.readBoolean()) {
         removeExpiredLifespan = input.readLong();
      }
   }

   private void initWithRemoveExpiredCommand(RemoveExpiredCommand command) {
      operation = Operation.REMOVE_EXPIRED;
      conditionalValue = command.getValue();
      removeExpiredLifespan = command.getLifespan();
   }

   private void initWithPutCommand(PutKeyValueCommand command) {
      operation = command.isPutIfAbsent() ? Operation.PUT_IF_ABSENT : Operation.PUT;
      value = command.getValue();
      metadata = command.getMetadata();
   }

   private void initWithReplaceCommand(ReplaceCommand command) {
      conditionalValue = command.getOldValue();
      operation = conditionalValue == null ? Operation.REPLACE : Operation.CONDITIONAL_REPLACE;
      value = command.getNewValue();
      metadata = command.getMetadata();
   }

   private void initWithRemoveCommand(RemoveCommand command) {
      conditionalValue = command.getValue();
      operation = conditionalValue == null ? Operation.REMOVE : Operation.CONDITIONAL_REMOVE;
   }

   private void initDataWriteCommand(DataWriteCommand command) {
      this.commandInvocationId = command.getCommandInvocationId();
      this.key = command.getKey();
      this.topologyId = command.getTopologyId();
      this.flags = command.getFlagsBitSet();
      this.valueMatcher = command.getValueMatcher();
   }

   private void writeConditionalValue(ObjectOutput output) throws IOException {
      if (operation == Operation.CONDITIONAL_REMOVE || operation == Operation.CONDITIONAL_REPLACE || operation == Operation.REMOVE_EXPIRED) {
         output.writeObject(conditionalValue);
      }
   }

   private void writeValue(ObjectOutput output) throws IOException {
      if (operation == Operation.REMOVE || operation == Operation.REMOVE_EXPIRED) {
         return; //no value
      }
      output.writeObject(value);
      output.writeObject(metadata);
   }

   private void readConditionalValue(ObjectInput input) throws IOException, ClassNotFoundException {
      if (operation == Operation.CONDITIONAL_REMOVE || operation == Operation.CONDITIONAL_REPLACE || operation == Operation.REMOVE_EXPIRED) {
         this.conditionalValue = input.readObject();
      }
   }

   private void readValue(ObjectInput input) throws IOException, ClassNotFoundException {
      if (operation == Operation.REMOVE || operation == Operation.REMOVE_EXPIRED) {
         return;
      }
      value = input.readObject();
      metadata = (Metadata) input.readObject();
   }

   private enum Operation {
      PUT,
      PUT_IF_ABSENT,
      REMOVE,
      CONDITIONAL_REMOVE,
      REMOVE_EXPIRED,
      REPLACE,
      CONDITIONAL_REPLACE
   }
}
