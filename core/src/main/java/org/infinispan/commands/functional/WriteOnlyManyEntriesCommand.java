package org.infinispan.commands.functional;

import static org.infinispan.util.TriangleFunctionsUtil.filterEntries;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commands.write.BackupMultiKeyWriteRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;

public final class WriteOnlyManyEntriesCommand<K, V> extends AbstractWriteManyCommand<K, V> {

   public static final byte COMMAND_ID = 57;

   private Map<?, ?> entries;
   private BiConsumer<V, WriteEntryView<V>> f;

   public WriteOnlyManyEntriesCommand(Map<?, ?> entries,
                                      BiConsumer<V, WriteEntryView<V>> f,
                                      Params params,
                                      CommandInvocationId commandInvocationId,
                                      DataConversion keyDataConversion,
                                      DataConversion valueDataConversion,
                                      ComponentRegistry componentRegistry) {
      super(commandInvocationId, params, keyDataConversion, valueDataConversion);
      this.entries = entries;
      this.f = f;
      init(componentRegistry);
   }

   public WriteOnlyManyEntriesCommand(WriteOnlyManyEntriesCommand<K, V> command) {
      super(command);
      this.entries = command.entries;
      this.f = command.f;
      this.keyDataConversion = command.keyDataConversion;
      this.valueDataConversion = command.valueDataConversion;
   }

   public WriteOnlyManyEntriesCommand() {
   }

   public Map<?, ?> getEntries() {
      return entries;
   }

   public void setEntries(Map<?, ?> entries) {
      this.entries = entries;
   }

   public final WriteOnlyManyEntriesCommand<K, V> withEntries(Map<?, ?> entries) {
      setEntries(entries);
      return this;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      MarshallUtil.marshallMap(entries, output);
      output.writeObject(f);
      output.writeBoolean(isForwarded);
      Params.writeObject(output, params);
      output.writeInt(topologyId);
      output.writeLong(flags);
      DataConversion.writeTo(output, keyDataConversion);
      DataConversion.writeTo(output, valueDataConversion);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      // We use LinkedHashMap in order to guarantee the same order of iteration
      entries = MarshallUtil.unmarshallMap(input, LinkedHashMap::new);
      f = (BiConsumer<V, WriteEntryView<V>>) input.readObject();
      isForwarded = input.readBoolean();
      params = Params.readObject(input);
      topologyId = input.readInt();
      flags = input.readLong();
      keyDataConversion = DataConversion.readFrom(input);
      valueDataConversion = DataConversion.readFrom(input);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      for (Map.Entry<?, ?> entry : entries.entrySet()) {
         CacheEntry cacheEntry = ctx.lookupEntry(entry.getKey());

         // Could be that the key is not local, 'null' is how this is signalled
         if (cacheEntry == null) {
            throw new IllegalStateException();
         }
         V decodedValue = (V) valueDataConversion.fromStorage(entry.getValue());
         f.accept(decodedValue, EntryViews.writeOnly(cacheEntry, valueDataConversion));
      }
      return null;
   }

   @Override
   public boolean isReturnValueExpected() {
      // Scattered cache always needs some response.
      return true;
   }

   @Override
   public Collection<?> getAffectedKeys() {
      return entries.keySet();
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitWriteOnlyManyEntriesCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.DONT_LOAD;
   }

   @Override
   public boolean isWriteOnly() {
      return true;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("WriteOnlyManyEntriesCommand{");
      sb.append("entries=").append(entries);
      sb.append(", f=").append(f.getClass().getName());
      sb.append(", isForwarded=").append(isForwarded);
      sb.append(", keyDataConversion=").append(keyDataConversion);
      sb.append(", valueDataConversion=").append(valueDataConversion);
      sb.append('}');
      return sb.toString();
   }

   @Override
   public Collection<?> getKeysToLock() {
      return entries.keySet();
   }

   @Override
   public Mutation<K, V, ?> toMutation(Object key) {
      return new Mutations.WriteWithValue<>(keyDataConversion, valueDataConversion, entries.get(key), f);
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
      if (f instanceof InjectableComponent)
         ((InjectableComponent) f).inject(componentRegistry);
   }

   @Override
   public void initBackupMultiKeyWriteRpcCommand(BackupMultiKeyWriteRpcCommand command, Collection<Object> keys) {
      //noinspection unchecked
      command.setWriteOnlyEntries(commandInvocationId, filterEntries(entries, keys), f, params, getFlagsBitSet(), getTopologyId(), keyDataConversion, valueDataConversion);
   }
}
