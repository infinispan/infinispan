package org.infinispan.commands.triangle;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.BiFunction;
import java.util.function.Function;

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
         CacheNotifier cacheNotifier, ComponentRegistry componentRegistry, VersionGenerator versionGenerator) {
      injectDependencies(factory, chain);
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
   }

   public void setRemoveCommand(RemoveCommand command, boolean removeExpired) {
      this.operation = removeExpired ? Operation.REMOVE_EXPIRED : Operation.REMOVE;
      setCommonAttributesFromCommand(command);
      this.key = command.getKey();
      this.valueOrFunction = command.getValue();
   }

   public void setReplaceCommand(ReplaceCommand command) {
      this.operation = Operation.REPLACE;
      setCommonAttributesFromCommand(command);
      this.key = command.getKey();
      this.valueOrFunction = command.getNewValue();
      this.metadata = command.getMetadata();
   }

   public void setComputeCommand(ComputeCommand command) {
      this.operation = command.isComputeIfPresent() ? Operation.COMPUTE_IF_PRESENT : Operation.COMPUTE;
      setCommonAttributesFromCommand(command);
      this.key = command.getKey();
      this.valueOrFunction = command.getRemappingBiFunction();
      this.metadata = command.getMetadata();
   }

   public void setComputeIfAbsentCommand(ComputeIfAbsentCommand command) {
      this.operation = Operation.COMPUTE_IF_ABSENT;
      setCommonAttributesFromCommand(command);
      this.key = command.getKey();
      this.valueOrFunction = command.getMappingFunction();
      this.metadata = command.getMetadata();
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      writeBase(output);
      MarshallUtil.marshallEnum(operation, output);
      output.writeObject(key);
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
      switch (operation) {
         case REMOVE:
            return new RemoveCommand(key, null, cacheNotifier, getFlags(), getCommandInvocationId());
         case WRITE:
            return new PutKeyValueCommand(key, valueOrFunction, false, cacheNotifier, metadata, getTopologyId(),
                  getCommandInvocationId());
         case COMPUTE:
            return new ComputeCommand(key, (BiFunction) valueOrFunction, false, getFlags(), getCommandInvocationId(),
                  metadata, cacheNotifier, componentRegistry);
         case REPLACE:
            return new ReplaceCommand(key, null, valueOrFunction, cacheNotifier, metadata, getFlags(),
                  getCommandInvocationId());
         case REMOVE_EXPIRED:
            return new RemoveExpiredCommand(key, valueOrFunction, null, cacheNotifier, getCommandInvocationId(),
                  versionGenerator.nonExistingVersion());
         case COMPUTE_IF_PRESENT:
            return new ComputeCommand(key, (BiFunction) valueOrFunction, true, getFlags(), getCommandInvocationId(),
                  metadata, cacheNotifier, componentRegistry);
         case COMPUTE_IF_ABSENT:
            return new ComputeIfAbsentCommand(key, (Function) valueOrFunction, getFlags(), getCommandInvocationId(),
                  metadata, cacheNotifier, componentRegistry);
         default:
            throw new IllegalStateException("Unknown operation " + operation);
      }
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
