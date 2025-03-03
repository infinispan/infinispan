package org.infinispan.commands.functional;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.impl.Params;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

// TODO: the command does not carry previous values to backup, so it can cause
// the values on primary and backup owners to diverge in case of topology change
@ProtoTypeId(ProtoStreamTypeIds.READ_WRITE_MANY_ENTRIES_COMMAND)
public final class ReadWriteManyEntriesCommand<K, V, T, R> extends AbstractWriteManyCommand<K, V> {

   private Map<?, ?> arguments;
   private BiFunction<T, ReadWriteEntryView<K, V>, R> f;

   public ReadWriteManyEntriesCommand(Map<?, ?> arguments,
                                      BiFunction<T, ReadWriteEntryView<K, V>, R> f,
                                      Params params,
                                      CommandInvocationId commandInvocationId,
                                      DataConversion keyDataConversion,
                                      DataConversion valueDataConversion) {
      super(commandInvocationId, params, keyDataConversion, valueDataConversion);
      this.arguments = arguments;
      this.f = f;
   }

   public ReadWriteManyEntriesCommand(ReadWriteManyEntriesCommand command) {
      super(command);
      this.arguments = command.arguments;
      this.f = command.f;
   }

   @ProtoFactory
   ReadWriteManyEntriesCommand(CommandInvocationId commandInvocationId, boolean forwarded, int topologyId,
                               Params params, long flags, DataConversion keyDataConversion,
                               DataConversion valueDataConversion, MarshallableMap<?, ?> wrappedArguments,
                               MarshallableObject<BiFunction<T, ReadWriteEntryView<K, V>, R>> wrappedBiFunction,
                               MarshallableMap<Object, PrivateMetadata> internalMetadata) {
      super(commandInvocationId, forwarded, topologyId, params, flags, keyDataConversion, valueDataConversion, internalMetadata);
      this.arguments = MarshallableMap.unwrap(wrappedArguments);
      this.f = MarshallableObject.unwrap(wrappedBiFunction);
   }

   @ProtoField(number = 9, name = "arguments")
   MarshallableMap<?, ?> getWrappedArguments() {
      return MarshallableMap.create(arguments);
   }

   @ProtoField(number = 10, name = "bifunction")
   MarshallableObject<BiFunction<T, ReadWriteEntryView<K, V>, R>> getWrappedBiFunction() {
      return MarshallableObject.create(f);
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      super.init(componentRegistry);
      if (f instanceof InjectableComponent)
         ((InjectableComponent) f).inject(componentRegistry);
   }

   public BiFunction<T, ReadWriteEntryView<K, V>, R> getBiFunction() {
      return f;
   }

   public Map<?, ?> getArguments() {
      return arguments;
   }

   public void setArguments(Map<?, ?> arguments) {
      this.arguments = arguments;
      this.internalMetadataMap.keySet().retainAll(arguments.keySet());
   }

   public final ReadWriteManyEntriesCommand<K, V, T, R> withArguments(Map<?, ?> entries) {
      setArguments(entries);
      return this;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadWriteManyEntriesCommand(ctx, this);
   }

   @Override
   public Collection<?> getAffectedKeys() {
      return arguments.keySet();
   }

   public LoadType loadType() {
      return LoadType.OWNER;
   }

   @Override
   public String toString() {
      return "ReadWriteManyEntriesCommand{" + "arguments=" + arguments +
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
      return new Mutations.ReadWriteWithValue(keyDataConversion, valueDataConversion, arguments.get(key), f);
   }
}
