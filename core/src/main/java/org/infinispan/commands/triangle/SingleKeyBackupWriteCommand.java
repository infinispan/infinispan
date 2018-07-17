package org.infinispan.commands.triangle;

import java.io.IOException;
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
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
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
   public void writeTo(UserObjectOutput output) throws IOException {
      writeBase(output);
      MarshallUtil.marshallEnum(operation, output);
      switch (operation) {
         case COMPUTE_IF_PRESENT:
         case COMPUTE_IF_ABSENT:
         case COMPUTE:
            output.writeKey(key);
            // We must use the internal marshaller for functions
            output.writeObject(valueOrFunction);
            break;
         case REMOVE_EXPIRED:
         case REPLACE:
         case WRITE:
            output.writeEntry(key, valueOrFunction, metadata);
            break;
         case REMOVE:
            output.writeKey(key);
            break;
         default:
      }
   }

   @Override
   public void readFrom(UserObjectInput input) throws IOException, ClassNotFoundException {
      readBase(input);
      operation = MarshallUtil.unmarshallEnum(input, SingleKeyBackupWriteCommand::valueOf);
      key = input.readUserObject();
      switch (operation) {
         case COMPUTE_IF_PRESENT:
         case COMPUTE_IF_ABSENT:
         case COMPUTE:
            valueOrFunction = input.readObject();
            break;
         case REMOVE_EXPIRED:
         case REPLACE:
         case WRITE:
            metadata = (Metadata) input.readUserObject();
            valueOrFunction = input.readUserObject();
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
            return new RemoveCommand(key, null, cacheNotifier, segmentId, getFlags(), getCommandInvocationId());
         case WRITE:
            return new PutKeyValueCommand(key, valueOrFunction, false, cacheNotifier, metadata, segmentId, getTopologyId(),
                  getCommandInvocationId());
         case COMPUTE:
            return new ComputeCommand(key, (BiFunction) valueOrFunction, false, segmentId, getFlags(), getCommandInvocationId(),
                  metadata, cacheNotifier, componentRegistry);
         case REPLACE:
            return new ReplaceCommand(key, null, valueOrFunction, cacheNotifier, metadata, segmentId, getFlags(),
                  getCommandInvocationId());
         case REMOVE_EXPIRED:
            // Doesn't matter if it is max idle or not - important thing is that it raises expired event
            return new RemoveExpiredCommand(key, valueOrFunction, null, false, cacheNotifier, segmentId, getCommandInvocationId(),
                  versionGenerator.nonExistingVersion());
         case COMPUTE_IF_PRESENT:
            return new ComputeCommand(key, (BiFunction) valueOrFunction, true, segmentId, getFlags(), getCommandInvocationId(),
                  metadata, cacheNotifier, componentRegistry);
         case COMPUTE_IF_ABSENT:
            return new ComputeIfAbsentCommand(key, (Function) valueOrFunction, segmentId, getFlags(), getCommandInvocationId(),
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
