package org.infinispan.commands.functional;

import java.util.Collection;
import java.util.function.Function;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.impl.Params;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

// TODO: the command does not carry previous values to backup, so it can cause
// the values on primary and backup owners to diverge in case of topology change
@ProtoTypeId(ProtoStreamTypeIds.READ_WRITE_MANY_COMMAND)
public final class ReadWriteManyCommand<K, V, R> extends AbstractWriteManyCommand<K, V> {

   private Collection<?> keys;
   private Function<ReadWriteEntryView<K, V>, R> f;

   public ReadWriteManyCommand(Collection<?> keys, Function<ReadWriteEntryView<K, V>, R> f, Params params,
                               CommandInvocationId commandInvocationId, DataConversion keyDataConversion,
                               DataConversion valueDataConversion) {
      super(commandInvocationId, params, keyDataConversion, valueDataConversion);
      this.keys = keys;
      this.f = f;
   }

   public ReadWriteManyCommand(ReadWriteManyCommand command) {
      super(command);
      this.keys = command.keys;
      this.f = command.f;
   }

   @ProtoFactory
   ReadWriteManyCommand(CommandInvocationId commandInvocationId, boolean forwarded, int topologyId, Params params,
                        long flags, DataConversion keyDataConversion, DataConversion valueDataConversion,
                        MarshallableMap<Object, PrivateMetadata> internalMetadata, MarshallableCollection<?> keys,
                        MarshallableObject<Function<ReadWriteEntryView<K, V>, R>> wrappedFunction) {
      super(commandInvocationId, forwarded, topologyId, params, flags, keyDataConversion, valueDataConversion, internalMetadata);
      this.keys = MarshallableCollection.unwrap(keys);
      this.f = MarshallableObject.unwrap(wrappedFunction);
   }

   @ProtoField(9)
   MarshallableCollection<?> getKeys() {
      return MarshallableCollection.create(keys);
   }

   @ProtoField(number = 10, name = "function")
   MarshallableObject<Function<ReadWriteEntryView<K, V>, R>> getWrappedFunction() {
      return MarshallableObject.create(f);
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      super.init(componentRegistry);
      if (f instanceof InjectableComponent)
         ((InjectableComponent) f).inject(componentRegistry);
   }

   public Function<ReadWriteEntryView<K, V>, R> getFunction() {
      return f;
   }

   public void setKeys(Collection<?> keys) {
      this.keys = keys;
      this.internalMetadataMap.keySet().retainAll(keys);
   }

   public final ReadWriteManyCommand<K, V, R> withKeys(Collection<?> keys) {
      setKeys(keys);
      return this;
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
      return "ReadWriteManyCommand{" + "keys=" + keys +
            ", f=" + f +
            ", forwarded=" + forwarded +
            ", keyDataConversion=" + keyDataConversion +
            ", valueDataConversion=" + valueDataConversion +
            '}';
   }

   @Override
   public Collection<?> getKeysToLock() {
      return keys;
   }

   public Mutation toMutation(Object key) {
      return new Mutations.ReadWrite<>(keyDataConversion, valueDataConversion, f);
   }
}
