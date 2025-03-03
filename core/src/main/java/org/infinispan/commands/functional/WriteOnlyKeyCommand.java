package org.infinispan.commands.functional;

import java.util.function.Consumer;

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

@ProtoTypeId(ProtoStreamTypeIds.WRITE_ONLY_KEY_COMMAND)
public final class WriteOnlyKeyCommand<K, V> extends AbstractWriteKeyCommand<K, V> {

   private Consumer<WriteEntryView<K, V>> f;

   public WriteOnlyKeyCommand(Object key, Consumer<WriteEntryView<K, V>> f, int segment, CommandInvocationId id,
                              ValueMatcher valueMatcher, Params params, DataConversion keyDataConversion,
                              DataConversion valueDataConversion) {
      super(key, valueMatcher, segment, id, params, keyDataConversion, valueDataConversion);
      this.f = f;
   }

   @ProtoFactory
   WriteOnlyKeyCommand(MarshallableObject<?> wrappedKey, long flagsWithoutRemote, int topologyId, int segment,
                       CommandInvocationId commandInvocationId, Params params, ValueMatcher valueMatcher,
                       DataConversion keyDataConversion, DataConversion valueDataConversion,
                       MarshallableObject<Consumer<WriteEntryView<K, V>>> wrappedConsumer,
                       PrivateMetadata internalMetadata) {
      super(wrappedKey, flagsWithoutRemote, topologyId, segment, commandInvocationId, params, valueMatcher,
            keyDataConversion, valueDataConversion, internalMetadata);
      this.f = MarshallableObject.unwrap(wrappedConsumer);
   }

   @ProtoField(number = 11, name = "consumer")
   MarshallableObject<Consumer<WriteEntryView<K, V>>> getWrappedConsumer() {
      return MarshallableObject.create(f);
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      super.init(componentRegistry);
      if (f instanceof InjectableComponent)
         ((InjectableComponent) f).inject(componentRegistry);
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
