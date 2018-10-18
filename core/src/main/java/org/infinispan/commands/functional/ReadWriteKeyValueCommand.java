package org.infinispan.commands.functional;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.BiFunction;

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
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.impl.Params;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public final class ReadWriteKeyValueCommand<K, V, T, R> extends AbstractWriteKeyCommand<K, V> {
   private static final Log log = LogFactory.getLog(ReadWriteKeyValueCommand.class);

   public static final byte COMMAND_ID = 51;

   private Object argument;
   private BiFunction<T, ReadWriteEntryView<K, V>, R> f;
   private Object prevValue;
   private Metadata prevMetadata;

   public ReadWriteKeyValueCommand(Object key, Object argument, BiFunction<T, ReadWriteEntryView<K, V>, R> f,
                                   int segment, CommandInvocationId id,
                                   ValueMatcher valueMatcher,
                                   Params params,
                                   DataConversion keyDataConversion,
                                   DataConversion valueDataConversion,
                                   ComponentRegistry componentRegistry) {
      super(key, valueMatcher, segment, id, params, keyDataConversion, valueDataConversion);
      this.argument = argument;
      this.f = f;
      init(componentRegistry);
   }

   public ReadWriteKeyValueCommand() {
      // No-op, for marshalling
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeObject(argument);
      output.writeObject(f);
      MarshallUtil.marshallEnum(valueMatcher, output);
      UnsignedNumeric.writeUnsignedInt(output, segment);
      Params.writeObject(output, params);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeObject(prevValue);
      output.writeObject(prevMetadata);
      DataConversion.writeTo(output, keyDataConversion);
      DataConversion.writeTo(output, valueDataConversion);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      argument = input.readObject();
      f = (BiFunction<T, ReadWriteEntryView<K, V>, R>) input.readObject();
      valueMatcher = MarshallUtil.unmarshallEnum(input, ValueMatcher::valueOf);
      segment = UnsignedNumeric.readUnsignedInt(input);
      params = Params.readObject(input);
      setFlagsBitSet(input.readLong());
      commandInvocationId = CommandInvocationId.readFrom(input);
      prevValue = input.readObject();
      prevMetadata = (Metadata) input.readObject();
      keyDataConversion = DataConversion.readFrom(input);
      valueDataConversion = DataConversion.readFrom(input);
   }

   @Override
   public boolean isConditional() {
      return true;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadWriteKeyValueCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.OWNER;
   }

   @Override
   public String toString() {
      return new StringBuilder("ReadWriteKeyValueCommand{")
            .append("key=").append(toStr(key))
            .append(", argument=").append(toStr(argument))
            .append(", f=").append(f.getClass().getName())
            .append(", prevValue=").append(toStr(prevValue))
            .append(", prevMetadata=").append(toStr(prevMetadata))
            .append(", flags=").append(printFlags())
            .append(", commandInvocationId=").append(commandInvocationId)
            .append(", topologyId=").append(getTopologyId())
            .append(", valueMatcher=").append(valueMatcher)
            .append(", successful=").append(successful)
            .append(", keyDataConversion=").append(keyDataConversion)
            .append(", valueDataConversion=").append(valueDataConversion)
            .append("}")
            .toString();
   }

   @Override
   public Mutation toMutation(Object key) {
      return new Mutations.ReadWriteWithValue<>(keyDataConversion, valueDataConversion, argument, f);
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);

      if (f instanceof InjectableComponent)
         ((InjectableComponent) f).inject(componentRegistry);
   }

   public void setPrevValueAndMetadata(Object prevValue, Metadata prevMetadata) {
      this.prevMetadata = prevMetadata;
      this.prevValue = prevValue;
   }

   public Object getArgument() {
      return argument;
   }

   public BiFunction<T, ReadWriteEntryView<K, V>, R> getBiFunction() {
      return f;
   }

   public Object getPrevValue() {
      return prevValue;
   }

   public Metadata getPrevMetadata() {
      return prevMetadata;
   }
}
