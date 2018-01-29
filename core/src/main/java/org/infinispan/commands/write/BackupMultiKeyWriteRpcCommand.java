package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.impl.Params;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.ByteString;

/**
 * A command sent from the primary owner to the backup owners for a {@link PutMapCommand}.
 * <p>
 * This command is only visited by the backups owner and in a remote context. The command order is set by {@code
 * segmentsAndSequences} map.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class BackupMultiKeyWriteRpcCommand extends BaseRpcCommand implements TopologyAffectedCommand {

   public static final byte COMMAND_ID = 66;
   private static Operation[] CACHED_VALUES = Operation.values();

   private Operation operation;
   private CommandInvocationId commandInvocationId;
   private Object entries;
   private Metadata metadata;
   private long flags;
   private int topologyId;
   private long sequence;
   private Object function;
   private Params params;
   private DataConversion keyDataConversion;
   private DataConversion valueDataConversion;

   private InvocationContextFactory invocationContextFactory;
   private AsyncInterceptorChain interceptorChain;
   private CacheNotifier cacheNotifier;
   private KeyPartitioner keyPartitioner;
   private ComponentRegistry componentRegistry;

   //used for testing
   @SuppressWarnings("unused")
   public BackupMultiKeyWriteRpcCommand() {
      super(null);
   }

   public BackupMultiKeyWriteRpcCommand(ByteString cacheName) {
      super(cacheName);
   }

   private static Operation valueOf(int index) {
      return CACHED_VALUES[index];
   }

   public CommandInvocationId getCommandInvocationId() {
      return commandInvocationId;
   }

   public void init(InvocationContextFactory invocationContextFactory, AsyncInterceptorChain interceptorChain,
         CacheNotifier cacheNotifier, KeyPartitioner keyPartitioner, ComponentRegistry componentRegistry) {
      this.invocationContextFactory = invocationContextFactory;
      this.interceptorChain = interceptorChain;
      this.cacheNotifier = cacheNotifier;
      this.keyPartitioner = keyPartitioner;
      this.componentRegistry = componentRegistry;
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
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(flags));
      output.writeLong(sequence);

      switch (operation) {
         case PUT_MAP:
            MarshallUtil.marshallMap((Map<?, ?>) entries, output);
            output.writeObject(metadata);
            break;
         case WRITE_ONLY_ENTRIES:
         case READ_WRITE_ENTRIES:
            MarshallUtil.marshallMap((Map<?, ?>) entries, output);
            output.writeObject(function);
            Params.writeObject(output, params);
            DataConversion.writeTo(output, keyDataConversion);
            DataConversion.writeTo(output, valueDataConversion);
            break;
         case WRITE_ONLY:
         case READ_WRITE:
            MarshallUtil.marshallCollection((Collection<?>) entries, output);
            output.writeObject(function);
            Params.writeObject(output, params);
            DataConversion.writeTo(output, keyDataConversion);
            DataConversion.writeTo(output, valueDataConversion);
            break;
         default:
      }

   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      operation = MarshallUtil.unmarshallEnum(input, BackupMultiKeyWriteRpcCommand::valueOf);
      commandInvocationId = CommandInvocationId.readFrom(input);
      flags = input.readLong();
      sequence = input.readLong();

      switch (operation) {
         case PUT_MAP:
            entries = MarshallUtil.unmarshallMap(input, HashMap::new);
            metadata = (Metadata) input.readObject();
            break;
         case WRITE_ONLY_ENTRIES:
         case READ_WRITE_ENTRIES:
            entries = MarshallUtil.unmarshallMap(input, HashMap::new);
            function = input.readObject();
            params = Params.readObject(input);
            keyDataConversion = DataConversion.readFrom(input);
            valueDataConversion = DataConversion.readFrom(input);
            break;
         case WRITE_ONLY:
         case READ_WRITE:
            entries = MarshallUtil.unmarshallCollection(input, ArrayList::new);
            function = input.readObject();
            params = Params.readObject(input);
            keyDataConversion = DataConversion.readFrom(input);
            valueDataConversion = DataConversion.readFrom(input);
            break;
         default:
      }

   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      WriteCommand command;
      switch (operation) {
         case PUT_MAP:
            command = buildPutMapCommand();
            break;
         case WRITE_ONLY_ENTRIES:
            command = buildWriteOnlyManyEntriesCommand();
            break;
         case WRITE_ONLY:
            command = buildWriteOnlyManyCommand();
            break;
         case READ_WRITE:
            command = buildReadWriteManyCommand();
            break;
         case READ_WRITE_ENTRIES:
            command = buildReadWriteManyEntriesCommand();
            break;
         default:
            throw new IllegalStateException("Unknown operation: " + operation);
      }

      InvocationContext invocationContext = invocationContextFactory
            .createRemoteInvocationContextForCommand(command, getOrigin());
      return interceptorChain.invokeAsync(invocationContext, command);
   }

   @Override
   public String toString() {
      return "BackupMultiKeyWriteRpcCommand{" +
            "operation=" + operation +
            ", commandInvocationId=" + commandInvocationId +
            ", entries=" + entries +
            ", metadata=" + metadata +
            ", flags=" + EnumUtil.prettyPrintBitSet(flags, Flag.class) +
            ", topologyId=" + topologyId +
            ", sequence=" + sequence +
            ", function=" + function +
            ", params=" + params +
            ", keyDataConversion=" + keyDataConversion +
            ", valueDataConversion=" + valueDataConversion +
            ", cacheName=" + cacheName +
            '}';
   }

   public long getSequence() {
      return sequence;
   }

   public void setSequence(long sequence) {
      this.sequence = sequence;
   }

   public int getSegmentId() {
      return keyPartitioner.getSegment(anyKey());
   }

   public void setPutMap(CommandInvocationId commandInvocationId, Map map, Metadata metadata,
         long flags, int topologyId) {
      this.operation = Operation.PUT_MAP;
      setCommonAttributes(commandInvocationId, flags, topologyId);
      this.entries = map;
      this.metadata = metadata;
   }

   public <V> void setWriteOnlyEntries(CommandInvocationId commandInvocationId, Map map,
         BiConsumer<V, WriteEntryView<V>> biConsumer,
         Params params, long flags, int topologyId, DataConversion keyDataConversion,
         DataConversion valueDataConversion) {
      this.operation = Operation.WRITE_ONLY_ENTRIES;
      setCommonAttributes(commandInvocationId, flags, topologyId);
      this.entries = map;
      this.function = biConsumer;
      this.params = params;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   public <V> void setWriteOnly(CommandInvocationId commandInvocationId, Collection<?> keys,
         Consumer<WriteEntryView<V>> consumer,
         Params params, long flags, int topologyId, DataConversion keyDataConversion,
         DataConversion valueDataConversion) {
      this.operation = Operation.WRITE_ONLY;
      setCommonAttributes(commandInvocationId, flags, topologyId);
      this.entries = keys;
      this.function = consumer;
      this.params = params;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   public <K, V, R> void setReadWrite(CommandInvocationId commandInvocationId, Collection<Object> keys,
         Function<ReadWriteEntryView<K, V>, R> function, Params params, long flags, int topologyId,
         DataConversion keyDataConversion, DataConversion valueDataConversion) {
      this.operation = Operation.READ_WRITE;
      setCommonAttributes(commandInvocationId, flags, topologyId);
      this.entries = keys;
      this.function = function;
      this.params = params;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   public <K, V, R> void setReadWriteEntries(CommandInvocationId commandInvocationId, Map map,
         BiFunction<V, ReadWriteEntryView<K, V>, R> biFunction, Params params, long flags, int topologyId,
         DataConversion keyDataConversion, DataConversion valueDataConversion) {
      this.operation = Operation.READ_WRITE_ENTRIES;
      setCommonAttributes(commandInvocationId, flags, topologyId);
      this.entries = map;
      this.function = biFunction;
      this.params = params;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   public long getFlagBitSet() {
      return flags;
   }

   private ReadWriteManyEntriesCommand buildReadWriteManyEntriesCommand() {
      //noinspection unchecked
      ReadWriteManyEntriesCommand command = new ReadWriteManyEntriesCommand((Map<?, ?>) entries, (BiFunction) function,
            params, commandInvocationId, keyDataConversion, valueDataConversion, componentRegistry);
      command.setFlagsBitSet(flags);
      command.setTopologyId(topologyId);
      command.setForwarded(true);
      return command;
   }

   private ReadWriteManyCommand buildReadWriteManyCommand() {
      //noinspection unchecked
      ReadWriteManyCommand command = new ReadWriteManyCommand((Collection<?>) entries, (Function) function, params,
            commandInvocationId, keyDataConversion, keyDataConversion, componentRegistry);
      command.setFlagsBitSet(flags);
      command.setTopologyId(topologyId);
      command.setForwarded(true);
      return command;
   }

   private WriteOnlyManyEntriesCommand buildWriteOnlyManyEntriesCommand() {
      //noinspection unchecked
      WriteOnlyManyEntriesCommand command = new WriteOnlyManyEntriesCommand((Map<?, ?>) entries, (BiConsumer) function,
            params, commandInvocationId, keyDataConversion, valueDataConversion, componentRegistry);
      command.setFlagsBitSet(flags);
      command.setTopologyId(topologyId);
      command.setForwarded(true);
      return command;
   }

   private WriteOnlyManyCommand<?, ?> buildWriteOnlyManyCommand() {
      //noinspection unchecked
      WriteOnlyManyCommand command = new WriteOnlyManyCommand((Collection<?>) entries, (Consumer) function, params,
            commandInvocationId, keyDataConversion, keyDataConversion, componentRegistry);
      command.setFlagsBitSet(flags);
      command.setTopologyId(topologyId);
      command.setForwarded(true);
      return command;
   }

   private PutMapCommand buildPutMapCommand() {
      PutMapCommand command = new PutMapCommand((Map<?, ?>) entries, cacheNotifier, metadata, flags, commandInvocationId);
      command.addFlags(FlagBitSets.SKIP_LOCKING);
      command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
      command.setTopologyId(topologyId);
      command.setForwarded(true);
      return command;
   }

   private void setCommonAttributes(CommandInvocationId commandInvocationId, long flags, int topologyId) {
      this.commandInvocationId = commandInvocationId;
      this.flags = flags;
      this.topologyId = topologyId;
   }

   private Object anyKey() {
      switch (operation) {
         case PUT_MAP:
         case WRITE_ONLY_ENTRIES:
         case READ_WRITE_ENTRIES:
            return ((Map<?, ?>) entries).keySet().iterator().next();
         default:
            return ((Collection<?>) entries).iterator().next();
      }
   }

   private enum Operation {
      WRITE_ONLY_ENTRIES,
      WRITE_ONLY,
      READ_WRITE,
      READ_WRITE_ENTRIES,
      PUT_MAP
   }
}
