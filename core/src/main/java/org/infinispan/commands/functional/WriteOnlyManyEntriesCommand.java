package org.infinispan.commands.functional;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.impl.Params;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.WRITE_ONLY_MANY_ENTRIES_COMMAND)
public final class WriteOnlyManyEntriesCommand<K, V, T> extends AbstractWriteManyCommand<K, V> {

   private Map<?, ?> arguments;
   private BiConsumer<T, WriteEntryView<K, V>> f;

   public WriteOnlyManyEntriesCommand(Map<?, ?> arguments, BiConsumer<T, WriteEntryView<K, V>> f, Params params,
                                      CommandInvocationId commandInvocationId, DataConversion keyDataConversion,
                                      DataConversion valueDataConversion) {
      super(commandInvocationId, params, keyDataConversion, valueDataConversion);
      this.arguments = arguments;
      this.f = f;
   }

   public WriteOnlyManyEntriesCommand(WriteOnlyManyEntriesCommand<K, V, T> command) {
      super(command);
      this.arguments = command.arguments;
      this.f = command.f;
   }

   @ProtoFactory
   WriteOnlyManyEntriesCommand(CommandInvocationId commandInvocationId, boolean forwarded, int topologyId,
                               Params params, long flags, DataConversion keyDataConversion, DataConversion valueDataConversion,
                               MarshallableMap<Object, PrivateMetadata> internalMetadata, MarshallableMap<?, ?> wrappedArguments,
                               MarshallableObject<BiConsumer<T, WriteEntryView<K, V>>> wrappedBiConsumer) {
      super(commandInvocationId, forwarded, topologyId, params, flags, keyDataConversion, valueDataConversion, internalMetadata);
      this.arguments = MarshallableMap.unwrap(wrappedArguments);
      this.f = MarshallableObject.unwrap(wrappedBiConsumer);
   }

   @ProtoField(number = 9, name = "arguments")
   MarshallableMap<?, ?> getWrappedArguments() {
      return MarshallableMap.create(arguments);
   }

   @ProtoField(number = 10, name = "biconsumer")
   MarshallableObject<BiConsumer<T, WriteEntryView<K, V>>> getWrappedBiConsumer() {
      return MarshallableObject.create(f);
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      super.init(componentRegistry);
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
      this.internalMetadataMap.keySet().retainAll(arguments.keySet());
   }

   public final WriteOnlyManyEntriesCommand<K, V, T> withArguments(Map<?, ?> entries) {
      setArguments(entries);
      return this;
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
      return "WriteOnlyManyEntriesCommand{" + "arguments=" + arguments +
            ", f=" + f.getClass().getName() +
            ", forwarded=" + forwarded +
            ", keyDataConversion=" + keyDataConversion +
            ", valueDataConversion=" + valueDataConversion +
            '}';
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
