package org.infinispan.commands.triangle;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationManager;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.ByteString;

/**
 * A single key {@link BackupWriteCommand} for single key non-functional commands.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class SingleKeyBackupWriteCommand extends BackupWriteCommand {

   public static final byte COMMAND_ID = 76;
   private static final Operation[] CACHED_OPERATION = Operation.values();

   private Operation operation;
   private Object key;
   private Object valueOrFunction;
   private Metadata metadata;
   private CommandInvocationId lastInvocationId;

   private CacheNotifier cacheNotifier;
   private ComponentRegistry componentRegistry;
   private VersionGenerator versionGenerator;

   //for testing
   @SuppressWarnings("unused")
   public SingleKeyBackupWriteCommand() {
      super(null);
   }

   public SingleKeyBackupWriteCommand(ByteString cacheName) {
      super(cacheName);
   }

   private static Operation valueOf(int index) {
      return CACHED_OPERATION[index];
   }

   public void init(InvocationContextFactory factory, AsyncInterceptorChain chain,
                    CacheNotifier cacheNotifier, ComponentRegistry componentRegistry, VersionGenerator versionGenerator, InvocationManager invocationManager) {
      injectDependencies(factory, chain, invocationManager);
      this.cacheNotifier = cacheNotifier;
      this.componentRegistry = componentRegistry;
      this.versionGenerator = versionGenerator;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   public void setPutKeyValueCommand(PutKeyValueCommand command) {
      this.operation = Operation.WRITE;
      setCommonAttributesFromCommand(command);
      this.key = command.getKey();
      this.valueOrFunction = command.getValue();
      this.metadata = command.getMetadata();
      this.lastInvocationId = command.getLastInvocationId(command.getKey());
   }

   public void setRemoveCommand(RemoveCommand command, boolean removeExpired) {
      this.operation = removeExpired ? Operation.REMOVE_EXPIRED : Operation.REMOVE;
      setCommonAttributesFromCommand(command);
      this.key = command.getKey();
      this.valueOrFunction = command.getValue();
      this.lastInvocationId = command.getLastInvocationId(command.getKey());
   }

   public void setReplaceCommand(ReplaceCommand command) {
      this.operation = Operation.REPLACE;
      setCommonAttributesFromCommand(command);
      this.key = command.getKey();
      this.valueOrFunction = command.getNewValue();
      this.metadata = command.getMetadata();
      this.lastInvocationId = command.getLastInvocationId(command.getKey());
   }

   public void setComputeCommand(ComputeCommand command) {
      this.operation = command.isComputeIfPresent() ? Operation.COMPUTE_IF_PRESENT : Operation.COMPUTE;
      setCommonAttributesFromCommand(command);
      this.key = command.getKey();
      this.valueOrFunction = command.getRemappingBiFunction();
      this.metadata = command.getMetadata();
      this.lastInvocationId = command.getLastInvocationId(command.getKey());
   }

   public void setComputeIfAbsentCommand(ComputeIfAbsentCommand command) {
      this.operation = Operation.COMPUTE_IF_ABSENT;
      setCommonAttributesFromCommand(command);
      this.key = command.getKey();
      this.valueOrFunction = command.getMappingFunction();
      this.metadata = command.getMetadata();
      this.lastInvocationId = command.getLastInvocationId(command.getKey());
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      writeBase(output);
      MarshallUtil.marshallEnum(operation, output);
      output.writeObject(key);
      CommandInvocationId.writeTo(output, lastInvocationId);
      switch (operation) {
         case COMPUTE_IF_PRESENT:
         case COMPUTE_IF_ABSENT:
         case COMPUTE:
         case REPLACE:
         case WRITE:
            output.writeObject(metadata);
         case REMOVE_EXPIRED:
            output.writeObject(valueOrFunction);
            break;
         case REMOVE:
            break;
         default:
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      readBase(input);
      operation = MarshallUtil.unmarshallEnum(input, SingleKeyBackupWriteCommand::valueOf);
      key = input.readObject();
      lastInvocationId = CommandInvocationId.readFrom(input);
      switch (operation) {
         case COMPUTE_IF_PRESENT:
         case COMPUTE_IF_ABSENT:
         case COMPUTE:
         case REPLACE:
         case WRITE:
            metadata = (Metadata) input.readObject();
         case REMOVE_EXPIRED:
            valueOrFunction = input.readObject();
            break;
         case REMOVE:
            break;
         default:
      }
   }

   @Override
   public String toString() {
      return "SingleKeyBackupWriteCommand{" + toStringFields() + '}';
   }

   @Override
   WriteCommand createWriteCommand() {
      WriteCommand command;
      switch (operation) {
         case REMOVE:
            command = new RemoveCommand(key, null, cacheNotifier, getFlags(), getCommandInvocationId(), invocationManager);
            break;
         case WRITE:
            command = new PutKeyValueCommand(key, valueOrFunction, false, cacheNotifier, metadata, getTopologyId(),
                  getCommandInvocationId(), invocationManager);
            break;
         case COMPUTE:
            command = new ComputeCommand(key, (BiFunction) valueOrFunction, false, getFlags(), getCommandInvocationId(),
                  metadata, cacheNotifier, componentRegistry, invocationManager);
            break;
         case REPLACE:
            command = new ReplaceCommand(key, null, valueOrFunction, cacheNotifier, metadata, getFlags(),
                  getCommandInvocationId(), invocationManager);
            break;
         case REMOVE_EXPIRED:
            command = new RemoveExpiredCommand(key, valueOrFunction, null, cacheNotifier, getCommandInvocationId(),
                  versionGenerator.nonExistingVersion(), invocationManager);
            break;
         case COMPUTE_IF_PRESENT:
            command = new ComputeCommand(key, (BiFunction) valueOrFunction, true, getFlags(), getCommandInvocationId(),
                  metadata, cacheNotifier, componentRegistry, invocationManager);
            break;
         case COMPUTE_IF_ABSENT:
            command = new ComputeIfAbsentCommand(key, (Function) valueOrFunction, getFlags(), getCommandInvocationId(),
                  metadata, cacheNotifier, componentRegistry, invocationManager);
            break;
         default:
            throw new IllegalStateException("Unknown operation " + operation);
      }
      command.setLastInvocationId(key, lastInvocationId);
      return command;
   }

   @Override
   String toStringFields() {
      return super.toStringFields() +
            ", operation=" + operation +
            ", key=" + key +
            ", valueOrFunction=" + valueOrFunction +
            ", metadata=" + metadata;
   }

   private enum Operation {
      WRITE,
      REMOVE,
      REMOVE_EXPIRED,
      REPLACE,
      COMPUTE,
      COMPUTE_IF_PRESENT,
      COMPUTE_IF_ABSENT
   }
}
