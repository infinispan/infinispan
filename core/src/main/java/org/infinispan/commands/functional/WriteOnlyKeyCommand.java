package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Consumer;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.impl.Params;

public final class WriteOnlyKeyCommand<K, V> extends AbstractWriteKeyCommand<K, V> {

   public static final byte COMMAND_ID = 54;

   private Consumer<WriteEntryView<K, V>> f;

   public WriteOnlyKeyCommand(Object key,
                              Consumer<WriteEntryView<K, V>> f,
                              int segment, CommandInvocationId id,
                              ValueMatcher valueMatcher,
                              Params params,
                              DataConversion keyDataConversion,
                              DataConversion valueDataConversion) {
      super(key, valueMatcher, segment, id, params, keyDataConversion, valueDataConversion);
      this.f = f;
   }

   public WriteOnlyKeyCommand() {
   }

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      super.init(componentRegistry, isRemote);
      if (f instanceof InjectableComponent)
         ((InjectableComponent) f).inject(componentRegistry);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeObject(f);
      MarshallUtil.marshallEnum(valueMatcher, output);
      UnsignedNumeric.writeUnsignedInt(output, segment);
      Params.writeObject(output, params);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      CommandInvocationId.writeTo(output, commandInvocationId);
      DataConversion.writeTo(output, keyDataConversion);
      DataConversion.writeTo(output, valueDataConversion);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      f = (Consumer<WriteEntryView<K, V>>) input.readObject();
      valueMatcher = MarshallUtil.unmarshallEnum(input, ValueMatcher::valueOf);
      segment = UnsignedNumeric.readUnsignedInt(input);
      params = Params.readObject(input);
      setFlagsBitSet(input.readLong());
      commandInvocationId = CommandInvocationId.readFrom(input);
      keyDataConversion = DataConversion.readFrom(input);
      valueDataConversion = DataConversion.readFrom(input);
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitWriteOnlyKeyCommand(ctx, this);
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
   public Mutation<K, V, ?> toMutation(Object key) {
      return new Mutations.Write(keyDataConversion, valueDataConversion, f);
   }

   public Consumer<WriteEntryView<K, V>> getConsumer() {
      return f;
   }
}
