package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
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

public final class WriteOnlyManyCommand<K, V> extends AbstractWriteManyCommand<K, V> {

   public static final byte COMMAND_ID = 56;

   private Collection<?> keys;
   private Consumer<WriteEntryView<K, V>> f;

   public WriteOnlyManyCommand(Collection<?> keys,
                               Consumer<WriteEntryView<K, V>> f,
                               Params params,
                               CommandInvocationId commandInvocationId,
                               DataConversion keyDataConversion,
                               DataConversion valueDataConversion,
                               InvocationManager invocationManager,
                               ComponentRegistry componentRegistry) {
      super(commandInvocationId, params, keyDataConversion, valueDataConversion, invocationManager);
      this.keys = keys;
      this.f = f;
      init(componentRegistry);
   }

   public WriteOnlyManyCommand(WriteOnlyManyCommand<K, V> command) {
      super(command);
      this.keys = command.keys;
      this.f = command.f;
      this.keyDataConversion = command.keyDataConversion;
      this.valueDataConversion = command.valueDataConversion;
      this.invocationManager = command.invocationManager;
   }

   public WriteOnlyManyCommand() {
   }

   public Consumer<WriteEntryView<K, V>> getConsumer() {
      return f;
   }

   public void setKeys(Collection<?> keys) {
      this.keys = keys;
   }

   public final WriteOnlyManyCommand<K, V> withKeys(Collection<?> keys) {
      setKeys(keys);
      if (lastInvocationIds != null) {
         lastInvocationIds = lastInvocationIds.entrySet().stream()
               .filter(e -> keys.contains(e.getKey()))
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
      MarshallUtil.marshallCollection(keys, output);
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
      keys = MarshallUtil.unmarshallCollectionUnbounded(input, ArrayList::new);
      f = (Consumer<WriteEntryView<K, V>>) input.readObject();
      isForwarded = input.readBoolean();
      params = Params.readObject(input);
      flags = input.readLong();
      lastInvocationIds = MarshallUtil.unmarshallMap(input, in -> in.readObject(), CommandInvocationId::readFrom, HashMap::new);
      keyDataConversion = DataConversion.readFrom(input);
      valueDataConversion = DataConversion.readFrom(input);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitWriteOnlyManyCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      for (Object k : keys) {
         CacheEntry<K, V> cacheEntry = ctx.lookupEntry(k);
         Object prevValue = cacheEntry.getValue();
         Metadata prevMetadata = cacheEntry.getMetadata();
         f.accept(EntryViews.writeOnly(cacheEntry, valueDataConversion));
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
      return keys;
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
      final StringBuilder sb = new StringBuilder("WriteOnlyManyCommand{");
      sb.append("keys=").append(keys);
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
      return keys;
   }

   @Override
   public Mutation<K, V, ?> toMutation(Object key) {
      return new Mutations.Write<>(keyDataConversion, valueDataConversion, f);
   }

   @Override
   protected void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
      InjectableComponent.inject(componentRegistry, f);
   }

}
