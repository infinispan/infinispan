package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.impl.Params;

// TODO: the command does not carry previous values to backup, so it can cause
// the values on primary and backup owners to diverge in case of topology change
public final class ReadWriteManyCommand<K, V, R> extends AbstractWriteManyCommand<K, V> {

   public static final byte COMMAND_ID = 52;

   private Collection<?> keys;
   private Function<ReadWriteEntryView<K, V>, R> f;

   boolean isForwarded = false;

   public ReadWriteManyCommand(Collection<?> keys,
                               Function<ReadWriteEntryView<K, V>, R> f, Params params,
                               CommandInvocationId commandInvocationId,
                               DataConversion keyDataConversion,
                               DataConversion valueDataConversion,
                               ComponentRegistry componentRegistry) {
      super(commandInvocationId, params, keyDataConversion, valueDataConversion);
      this.keys = keys;
      this.f = f;
      init(componentRegistry);
   }

   public ReadWriteManyCommand(ReadWriteManyCommand command) {
      super(command);
      this.keys = command.keys;
      this.f = command.f;
      this.keyDataConversion = command.keyDataConversion;
      this.valueDataConversion = command.valueDataConversion;
   }

   public ReadWriteManyCommand() {
   }

   public Function<ReadWriteEntryView<K, V>, R> getFunction() {
      return f;
   }

   public void setKeys(Collection<?> keys) {
      this.keys = keys;
   }

   public final ReadWriteManyCommand<K, V, R> withKeys(Collection<?> keys) {
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
      keys = MarshallUtil.unmarshallCollection(input, ArrayList::new);
      f = (Function<ReadWriteEntryView<K, V>, R>) input.readObject();
      isForwarded = input.readBoolean();
      params = Params.readObject(input);
      topologyId = input.readInt();
      flags = input.readLong();
      keyDataConversion = DataConversion.readFrom(input);
      valueDataConversion = DataConversion.readFrom(input);
   }

   public boolean isForwarded() {
      return isForwarded;
   }

   public void setForwarded(boolean forwarded) {
      isForwarded = forwarded;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadWriteManyCommand(ctx, this);
   }

   @Override
   public Collection<?> getAffectedKeys() {
      return keys;
   }

   @Override
   public LoadType loadType() {
      return LoadType.OWNER;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("ReadWriteManyCommand{");
      sb.append("keys=").append(keys);
      sb.append(", f=").append(f);
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

   public Mutation toMutation(Object key) {
      return new Mutations.ReadWrite<>(keyDataConversion, valueDataConversion, f);
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
      if (f instanceof InjectableComponent) {
         ((InjectableComponent) f).inject(componentRegistry);
      }
   }

}
