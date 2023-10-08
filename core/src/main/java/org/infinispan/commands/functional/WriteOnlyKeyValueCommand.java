package org.infinispan.commands.functional;

import java.util.function.BiConsumer;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.impl.Params;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.WRITE_ONLY_KEY_VALUE_COMMAND)
public final class WriteOnlyKeyValueCommand<K, V, T> extends AbstractWriteKeyCommand<K, V> {

   public static final byte COMMAND_ID = 55;

   private BiConsumer<T, WriteEntryView<K, V>> f;
   private Object argument;

   public WriteOnlyKeyValueCommand(Object key, Object argument, BiConsumer<T, WriteEntryView<K, V>> f, int segment,
                                   CommandInvocationId id, ValueMatcher valueMatcher, Params params,
                                   DataConversion keyDataConversion, DataConversion valueDataConversion) {
      super(key, valueMatcher, segment, id, params, keyDataConversion, valueDataConversion);
      this.f = f;
      this.argument = argument;
   }

   @ProtoFactory
   WriteOnlyKeyValueCommand(MarshallableObject<?> wrappedKey, long flagsWithoutRemote, int topologyId, int segment,
                            CommandInvocationId commandInvocationId, Params params, ValueMatcher valueMatcher,
                            DataConversion keyDataConversion, DataConversion valueDataConversion, PrivateMetadata internalMetadata,
                            MarshallableObject<BiConsumer<T, WriteEntryView<K, V>>> wrappedBiConsumer,
                            MarshallableObject<?> wrappedArgument) {
      super(wrappedKey, flagsWithoutRemote, topologyId, segment, commandInvocationId, params, valueMatcher,
            keyDataConversion, valueDataConversion, internalMetadata);
      this.f = MarshallableObject.unwrap(wrappedBiConsumer);
      this.argument = MarshallableObject.unwrap(wrappedArgument);
   }

   @ProtoField(number = 11, name = "biconsumer")
   MarshallableObject<BiConsumer<T, WriteEntryView<K, V>>> getWrappedBiConsumer() {
      return MarshallableObject.create(f);
   }

   @ProtoField(number = 12, name = "argument")
   MarshallableObject<?> getWrappedArgument() {
      return  MarshallableObject.create(argument);
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
      return false;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitWriteOnlyKeyValueCommand(ctx, this);
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
      return new Mutations.WriteWithValue<>(keyDataConversion, valueDataConversion, argument, f);
   }

   public BiConsumer<T, WriteEntryView<K, V>> getBiConsumer() {
      return f;
   }

   public Object getArgument() {
      return argument;
   }
}
