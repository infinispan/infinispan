package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationManager;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;
import org.infinispan.metadata.Metadata;

public final class WriteOnlyManyEntriesCommand<K, V, T> extends AbstractWriteManyCommand<K, V> {

   public static final byte COMMAND_ID = 57;

   private Map<?, ?> arguments;
   private BiConsumer<T, WriteEntryView<K, V>> f;

   public WriteOnlyManyEntriesCommand(Map<?, ?> arguments,
                                      BiConsumer<T, WriteEntryView<K, V>> f,
                                      Params params,
                                      CommandInvocationId commandInvocationId,
                                      DataConversion keyDataConversion,
                                      DataConversion valueDataConversion,
                                      InvocationManager invocationManager,
                                      ComponentRegistry componentRegistry) {
      super(commandInvocationId, params, keyDataConversion, valueDataConversion, invocationManager);
      this.arguments = arguments;
      this.f = f;
      init(componentRegistry);
   }

   public WriteOnlyManyEntriesCommand(WriteOnlyManyEntriesCommand<K, V, T> command) {
      super(command);
      this.arguments = command.arguments;
      this.f = command.f;
      this.keyDataConversion = command.keyDataConversion;
      this.valueDataConversion = command.valueDataConversion;
   }

   public WriteOnlyManyEntriesCommand() {
   }

   public BiConsumer<T, WriteEntryView<K, V>> getBiConsumer() {
      return f;
   }

   public Map<?, ?> getArguments() {
      return arguments;
   }

   public void setArguments(Map<?, ?> arguments) {
      this.arguments = arguments;
   }

   public final WriteOnlyManyEntriesCommand<K, V, T> withArguments(Map<?, ?> arguments) {
      setArguments(arguments);
      if (lastInvocationIds != null) {
         lastInvocationIds = lastInvocationIds.entrySet().stream()
               .filter(e -> arguments.containsKey(e.getKey()))
               .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }
      return this;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      MarshallUtil.marshallMap(arguments, output);
      output.writeObject(f);
      output.writeBoolean(isForwarded);
      Params.writeObject(output, params);
      output.writeLong(flags);
      MarshallUtil.marshallMap(lastInvocationIds, ObjectOutput::writeObject, CommandInvocationId::writeTo, output);
      DataConversion.writeTo(output, keyDataConversion);
      DataConversion.writeTo(output, valueDataConversion);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      // We use LinkedHashMap in order to guarantee the same order of iteration
      arguments = MarshallUtil.unmarshallMap(input, LinkedHashMap::new);
      f = (BiConsumer<T, WriteEntryView<K, V>>) input.readObject();
      isForwarded = input.readBoolean();
      params = Params.readObject(input);
      flags = input.readLong();
      lastInvocationIds = MarshallUtil.unmarshallMap(input, in -> in.readObject(), CommandInvocationId::readFrom, HashMap::new);
      keyDataConversion = DataConversion.readFrom(input);
      valueDataConversion = DataConversion.readFrom(input);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      for (Map.Entry<?, ?> entry : arguments.entrySet()) {
         CacheEntry<K, V> cacheEntry = ctx.lookupEntry(entry.getKey());
         Object prevValue = cacheEntry.getValue();
         Metadata prevMetadata = cacheEntry.getMetadata();
         T decodedArgument = (T) valueDataConversion.fromStorage(entry.getValue());
         f.accept(decodedArgument, EntryViews.writeOnly(cacheEntry, valueDataConversion));
         recordInvocation(ctx, cacheEntry, prevValue, prevMetadata);
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
      return arguments.keySet();
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
      sb.append("arguments=").append(arguments);
      sb.append(", f=").append(f.getClass().getName());
      sb.append(", isForwarded=").append(isForwarded);
      sb.append(", keyDataConversion=").append(keyDataConversion);
      sb.append(", valueDataConversion=").append(valueDataConversion);
      sb.append(", topologyId=").append(topologyId);
      sb.append(", commandInvocationId=").append(commandInvocationId);
      sb.append('}');
      return sb.toString();
   }

   @Override
   public Collection<?> getKeysToLock() {
      return arguments.keySet();
   }

   @Override
   public Mutation<K, V, ?> toMutation(Object key) {
      return new Mutations.WriteWithValue<>(keyDataConversion, valueDataConversion, arguments.get(key), f);
   }

   @Override
   protected void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
      InjectableComponent.inject(componentRegistry, f);
   }

}
