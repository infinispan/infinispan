package org.infinispan.commands.functional;

import static org.infinispan.commons.util.Util.toStr;

import java.util.function.BiFunction;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.impl.Params;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.READ_WRITE_KEY_VALUE_COMMAND)
public final class ReadWriteKeyValueCommand<K, V, T, R> extends AbstractWriteKeyCommand<K, V> {

   public static final byte COMMAND_ID = 51;

   private Object argument;
   private BiFunction<T, ReadWriteEntryView<K, V>, R> f;
   private Object prevValue;
   private Metadata prevMetadata;

   public ReadWriteKeyValueCommand(Object key, Object argument, BiFunction<T, ReadWriteEntryView<K, V>, R> f,
                                   int segment, CommandInvocationId id, ValueMatcher valueMatcher, Params params,
                                   DataConversion keyDataConversion, DataConversion valueDataConversion) {
      super(key, valueMatcher, segment, id, params, keyDataConversion, valueDataConversion);
      this.argument = argument;
      this.f = f;
   }

   @ProtoFactory
   ReadWriteKeyValueCommand(MarshallableObject<?> wrappedKey, long flagsWithoutRemote, int topologyId, int segment,
                            CommandInvocationId commandInvocationId, Params params, ValueMatcher valueMatcher,
                            DataConversion keyDataConversion, DataConversion valueDataConversion,
                            MarshallableObject<?> wrappedArgument,
                            MarshallableObject<BiFunction<T, ReadWriteEntryView<K, V>, R>> wrappedFunction,
                            MarshallableObject<?> wrappedPrevValue, MarshallableObject<Metadata> wrappedPrevMetadata,
                            PrivateMetadata internalMetadata) {
      super(wrappedKey, flagsWithoutRemote, topologyId, segment, commandInvocationId, params, valueMatcher,
            keyDataConversion, valueDataConversion, internalMetadata);
      this.argument = MarshallableObject.unwrap(wrappedArgument);
      this.f = MarshallableObject.unwrap(wrappedFunction);
      this.prevValue = MarshallableObject.unwrap(wrappedPrevValue);
      this.prevMetadata = MarshallableObject.unwrap(wrappedPrevMetadata);
   }

   @ProtoField(number = 11, name = "argument")
   MarshallableObject<?> getWrappedArgument() {
      return MarshallableObject.create(argument);
   }

   @ProtoField(number = 12, name = "function")
   MarshallableObject<BiFunction<T, ReadWriteEntryView<K, V>, R>> getWrappedFunction() {
      return MarshallableObject.create(f);
   }

   @ProtoField(number = 13)
   MarshallableObject<?> getWrappedPrevValue() {
      return MarshallableObject.create(prevValue);
   }

   @ProtoField(number = 14)
   MarshallableObject<Metadata> getWrappedPrevMetadata() {
      return MarshallableObject.create(prevMetadata);
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      super.init(componentRegistry);
      if (f instanceof InjectableComponent)
         ((InjectableComponent) f).inject(componentRegistry);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
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
      return "ReadWriteKeyValueCommand{" +
            "key=" + toStr(key) +
            ", argument=" + toStr(argument) +
            ", f=" + getBiFunction().getClass().getName() +
            ", prevValue=" + toStr(prevValue) +
            ", prevMetadata=" + toStr(prevMetadata) +
            ", flags=" + printFlags() +
            ", commandInvocationId=" + commandInvocationId +
            ", topologyId=" + getTopologyId() +
            ", valueMatcher=" + valueMatcher +
            ", successful=" + successful +
            ", keyDataConversion=" + keyDataConversion +
            ", valueDataConversion=" + valueDataConversion +
            "}";
   }

   @Override
   public Mutation<K, V, R> toMutation(Object key) {
      return new Mutations.ReadWriteWithValue<>(keyDataConversion, valueDataConversion, argument, f);
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
