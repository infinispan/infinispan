package org.infinispan.commands.write;

import static org.infinispan.commands.write.ValueMatcher.MATCH_ALWAYS;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.impl.Params;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.ByteString;

/**
 * A command sent from the primary owner to the backup owners of a key with the new update.
 * <p>
 * This command is only visited by the backups owner and in a remote context. No locks are acquired since it is sent in
 * FIFO order, set by {@code sequence}. It can represent a update or remove operation.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class BackupWriteRpcCommand extends BaseRpcCommand implements TopologyAffectedCommand {

   public static final byte COMMAND_ID = 61;
   private static final Operation[] CACHED_VALUES = Operation.values();

   private Operation operation;
   private CommandInvocationId commandInvocationId;
   private Object key;
   private Object value;
   private Object function;
   private Metadata metadata;
   private int topologyId;
   private long flags;
   private long sequence;
   private Params params;
   private DataConversion keyDataConversion;
   private DataConversion valueDataConversion;
   private Object prevValue;
   private Metadata prevMetadata;

   private InvocationContextFactory invocationContextFactory;
   private AsyncInterceptorChain interceptorChain;
   private CacheNotifier cacheNotifier;
   private ComponentRegistry componentRegistry;
   private VersionGenerator versionGenerator;
   private KeyPartitioner keyPartitioner;

   //for org.infinispan.commands.CommandIdUniquenessTest
   public BackupWriteRpcCommand() {
      super(null);
   }

   public BackupWriteRpcCommand(ByteString cacheName) {
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

   public void setCompute(CommandInvocationId id, Object key, BiFunction remappingFunction, boolean computeIfPresent, Metadata metadata, long flags,
                          int topologyId) {
      this.operation = computeIfPresent ? Operation.COMPUTE_IF_PRESENT : Operation.COMPUTE;
      setCommonAttributes(id, key, flags, topologyId);
      this.function = remappingFunction;
      this.metadata = metadata;
   }

   public void setComputeIfAbsent(CommandInvocationId id, Object key, Function mappingFunction, Metadata metadata, long flags, int topologyId) {
      this.operation = Operation.COMPUTE_IF_ABSENT;
      setCommonAttributes(id, key, flags, topologyId);
      this.function = mappingFunction;
      this.metadata = metadata;
   }

   public void setReadWriteKey(CommandInvocationId commandInvocationId, Object key, Function function, Params params,
         long flags, int topologyId, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      this.operation = Operation.READ_WRITE_KEY;
      setCommonAttributes(commandInvocationId, key, flags, topologyId);
      this.function = function;
      this.params = params;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   public void setReadWriteKeyValue(CommandInvocationId commandInvocationId, Object key, BiFunction biFunction,
         Object value, Object prevValue, Metadata prevMetadata, Params params, long flags, int topologyId,
         DataConversion keyDataConversion, DataConversion valueDataConversion) {
      this.operation = Operation.READ_WRITE_KEY_VALUE;
      setCommonAttributes(commandInvocationId, key, flags, topologyId);
      this.function = biFunction;
      this.value = value;
      this.prevValue = prevValue;
      this.prevMetadata = prevMetadata;
      this.params = params;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   public void setWriteOnlyKey(CommandInvocationId commandInvocationId, Object key, Consumer consumer, Params params,
         long flags, int topologyId, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      this.operation = Operation.WRITE_ONLY_KEY;
      setCommonAttributes(commandInvocationId, key, flags, topologyId);
      this.function = consumer;
      this.params = params;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   public void setWriteOnlyKeyValue(CommandInvocationId commandInvocationId, Object key,
         BiConsumer biConsumer, Object value, Params params, long flags, int topologyId,
         DataConversion keyDataConversion, DataConversion valueDataConversion) {
      this.operation = Operation.WRITE_ONLY_KEY_VALUE;
      setCommonAttributes(commandInvocationId, key, flags, topologyId);
      this.function = biConsumer;
      this.value = value;
      this.params = params;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   public void init(InvocationContextFactory invocationContextFactory, AsyncInterceptorChain interceptorChain,
         CacheNotifier cacheNotifier, ComponentRegistry componentRegistry, VersionGenerator versionGenerator,
         KeyPartitioner keyPartitioner) {
      this.invocationContextFactory = invocationContextFactory;
      this.interceptorChain = interceptorChain;
      this.cacheNotifier = cacheNotifier;
      this.componentRegistry = componentRegistry;
      this.versionGenerator = versionGenerator;
      this.keyPartitioner = keyPartitioner;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      DataWriteCommand command;
      switch (operation) {
         case REMOVE:
            command = new RemoveCommand(key, null, cacheNotifier, flags, commandInvocationId);
            break;
         case REMOVE_EXPIRED:
            command = new RemoveExpiredCommand(key, value, null, cacheNotifier, commandInvocationId,
                  versionGenerator.nonExistingVersion());
            break;
         case WRITE:
            command = new PutKeyValueCommand(key, value, false, cacheNotifier, metadata, flags,
                  commandInvocationId);
            break;
         case REPLACE:
            command = new ReplaceCommand(key, null, value, cacheNotifier, metadata, flags, commandInvocationId);
            break;
         case COMPUTE:
            command = new ComputeCommand(key, (BiFunction) function, false, flags, commandInvocationId, metadata,
                  cacheNotifier, componentRegistry);
            break;
         case COMPUTE_IF_PRESENT:
            command = new ComputeCommand(key, (BiFunction) function, true, flags, commandInvocationId, metadata,
                  cacheNotifier, componentRegistry);
            break;
         case COMPUTE_IF_ABSENT:
            command = new ComputeIfAbsentCommand(key, (Function) function, flags, commandInvocationId, metadata,
                  cacheNotifier, componentRegistry);
            break;
         case READ_WRITE_KEY:
            //noinspection unchecked
            command = new ReadWriteKeyCommand(key, (Function) function, commandInvocationId, MATCH_ALWAYS,
                  params,
                  keyDataConversion, valueDataConversion, componentRegistry);
            break;
         case READ_WRITE_KEY_VALUE:
            command = createReadWriteKeyValueCommand();
            break;
         case WRITE_ONLY_KEY:
            //noinspection unchecked
            command = new WriteOnlyKeyCommand(key, (Consumer) function, commandInvocationId, MATCH_ALWAYS,
                  params,
                  keyDataConversion, valueDataConversion, componentRegistry);
            break;
         case WRITE_ONLY_KEY_VALUE:
            //noinspection unchecked
            command = new WriteOnlyKeyValueCommand(key, value, (BiConsumer) function, commandInvocationId,
                  MATCH_ALWAYS, params, keyDataConversion, valueDataConversion, componentRegistry);
            break;
         default:
            throw new IllegalStateException();
      }
      command.addFlags(FlagBitSets.SKIP_LOCKING);
      command.setValueMatcher(MATCH_ALWAYS);
      command.setTopologyId(topologyId);
      InvocationContext invocationContext = invocationContextFactory
            .createRemoteInvocationContextForCommand(command, getOrigin());
      return interceptorChain.invokeAsync(invocationContext, command);
   }

   public Object getKey() {
      return key;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   public CommandInvocationId getCommandInvocationId() {
      return commandInvocationId;
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
         case COMPUTE:
         case COMPUTE_IF_PRESENT:
         case COMPUTE_IF_ABSENT:
            output.writeObject(metadata);
            output.writeObject(function);
            break;
         case WRITE_ONLY_KEY_VALUE:
            output.writeObject(function);
            Params.writeObject(output, params);
            output.writeObject(value);
            DataConversion.writeTo(output, keyDataConversion);
            DataConversion.writeTo(output, valueDataConversion);
            break;
         case READ_WRITE_KEY_VALUE:
            output.writeObject(value);
            output.writeObject(function);
            Params.writeObject(output, params);
            output.writeObject(prevValue);
            output.writeObject(prevMetadata);
            DataConversion.writeTo(output, keyDataConversion);
            DataConversion.writeTo(output, valueDataConversion);
            break;
         case READ_WRITE_KEY:
         case WRITE_ONLY_KEY:
            output.writeObject(function);
            Params.writeObject(output, params);
            DataConversion.writeTo(output, keyDataConversion);
            DataConversion.writeTo(output, valueDataConversion);
            break;
         default:
      }
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(flags));
      output.writeLong(sequence);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      operation = MarshallUtil.unmarshallEnum(input, BackupWriteRpcCommand::valueOf);
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
         case COMPUTE:
         case COMPUTE_IF_PRESENT:
         case COMPUTE_IF_ABSENT:
            metadata = (Metadata) input.readObject();
            function = input.readObject();
            break;
         case WRITE_ONLY_KEY_VALUE:
            function = input.readObject();
            params = Params.readObject(input);
            value = input.readObject();
            keyDataConversion = DataConversion.readFrom(input);
            valueDataConversion = DataConversion.readFrom(input);
            break;
         case WRITE_ONLY_KEY:
         case READ_WRITE_KEY:
            function = input.readObject();
            params = Params.readObject(input);
            keyDataConversion = DataConversion.readFrom(input);
            valueDataConversion = DataConversion.readFrom(input);
            break;
         case READ_WRITE_KEY_VALUE:
            value = input.readObject();
            function = input.readObject();
            params = Params.readObject(input);
            prevValue = input.readObject();
            prevMetadata = (Metadata) input.readObject();
            keyDataConversion = DataConversion.readFrom(input);
            valueDataConversion = DataConversion.readFrom(input);
            break;
         default:
      }
      this.flags = input.readLong();
      this.sequence = input.readLong();
   }

   @Override
   public String toString() {
      return "BackupWriteRpcCommand{" +
            "operation=" + operation +
            ", commandInvocationId=" + commandInvocationId +
            ", key=" + key +
            ", value=" + value +
            ", function=" + function +
            ", metadata=" + metadata +
            ", topologyId=" + topologyId +
            ", flags=" + EnumUtil.prettyPrintBitSet(flags, Flag.class) +
            ", sequence=" + sequence +
            ", params=" + params +
            ", keyDataConversion=" + keyDataConversion +
            ", valueDataConversion=" + valueDataConversion +
            ", prevValue=" + prevValue +
            ", prevMetadata=" + prevMetadata +
            ", cacheName=" + cacheName +
            '}';
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   public long getSequence() {
      return sequence;
   }

   public void setSequence(long sequence) {
      this.sequence = sequence;
   }

   public long getFlagBitSet() {
      return flags;
   }

   public int getSegmentId() {
      return keyPartitioner.getSegment(key);
   }

   private void setCommonAttributes(CommandInvocationId commandInvocationId, Object key, long flags, int topologyId) {
      this.commandInvocationId = commandInvocationId;
      this.key = key;
      this.flags = flags;
      this.topologyId = topologyId;
   }

   private ReadWriteKeyValueCommand createReadWriteKeyValueCommand() {
      //noinspection unchecked
      ReadWriteKeyValueCommand cmd = new ReadWriteKeyValueCommand(key, value, (BiFunction) function,
            commandInvocationId, MATCH_ALWAYS, params, keyDataConversion, valueDataConversion, componentRegistry);
      cmd.setPrevValueAndMetadata(prevValue, prevMetadata);
      return cmd;
   }

   private enum Operation {
      WRITE,
      REMOVE,
      REMOVE_EXPIRED,
      REPLACE,
      COMPUTE,
      COMPUTE_IF_PRESENT,
      READ_WRITE_KEY,
      READ_WRITE_KEY_VALUE,
      WRITE_ONLY_KEY,
      WRITE_ONLY_KEY_VALUE,
      COMPUTE_IF_ABSENT
   }
}
