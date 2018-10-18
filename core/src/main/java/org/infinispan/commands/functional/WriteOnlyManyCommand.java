package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Consumer;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.impl.Params;

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
                               ComponentRegistry componentRegistry) {
      super(commandInvocationId, params, keyDataConversion, valueDataConversion);
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
      output.writeInt(topologyId);
      output.writeLong(flags);
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
      topologyId = input.readInt();
      flags = input.readLong();
      keyDataConversion = DataConversion.readFrom(input);
      valueDataConversion = DataConversion.readFrom(input);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitWriteOnlyManyCommand(ctx, this);
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
   public void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
      if (f instanceof InjectableComponent)
         ((InjectableComponent) f).inject(componentRegistry);
   }

}
