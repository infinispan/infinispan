package org.infinispan.commands.functional;

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
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.impl.Params;

public final class WriteOnlyManyEntriesCommand<K, V, T> extends AbstractWriteManyCommand<K, V> {

   public static final byte COMMAND_ID = 57;

   private Map<?, ?> arguments;
   private BiConsumer<T, WriteEntryView<K, V>> f;

   public WriteOnlyManyEntriesCommand(Map<?, ?> arguments,
                                      BiConsumer<T, WriteEntryView<K, V>> f,
                                      Params params,
                                      CommandInvocationId commandInvocationId,
                                      DataConversion keyDataConversion,
                                      DataConversion valueDataConversion) {
      super(commandInvocationId, params, keyDataConversion, valueDataConversion);
      this.arguments = arguments;
      this.f = f;
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

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      super.init(componentRegistry, isRemote);
      if (f instanceof InjectableComponent)
         ((InjectableComponent) f).inject(componentRegistry);
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

   public final WriteOnlyManyEntriesCommand<K, V, T> withArguments(Map<?, ?> entries) {
      setArguments(entries);
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
      output.writeInt(topologyId);
      output.writeLong(flags);
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
      topologyId = input.readInt();
      flags = input.readLong();
      keyDataConversion = DataConversion.readFrom(input);
      valueDataConversion = DataConversion.readFrom(input);
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
}
