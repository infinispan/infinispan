package org.infinispan.commands.functional;

import java.util.Collection;
import java.util.function.Function;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.impl.Params;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.READ_ONLY_MANY_COMMAND)
public class ReadOnlyManyCommand<K, V, R> extends AbstractTopologyAffectedCommand {

   protected Collection<?> keys;
   protected Function<ReadEntryView<K, V>, R> f;
   protected Params params;
   protected DataConversion keyDataConversion;
   protected DataConversion valueDataConversion;

   public ReadOnlyManyCommand(Collection<?> keys,
                              Function<ReadEntryView<K, V>, R> f,
                              Params params,
                              DataConversion keyDataConversion,
                              DataConversion valueDataConversion) {
      super(params.toFlagsBitSet(), -1);
      this.keys = keys;
      this.f = f;
      this.params = params;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   public ReadOnlyManyCommand(ReadOnlyManyCommand<K, V, R> c) {
      this(c.keys, c.f, c.params, c.keyDataConversion, c.valueDataConversion);
      this.setFlagsBitSet(c.getFlagsBitSet());
      this.topologyId = c.topologyId;
   }

   @ProtoFactory
   ReadOnlyManyCommand(long flagsWithoutRemote, int topologyId, MarshallableCollection<?> wrappedKeys,
                       MarshallableObject<Function<ReadEntryView<K, V>, R>> wrappedFunction, Params params,
                       DataConversion keyDataConversion, DataConversion valueDataConversion) {
      super(flagsWithoutRemote, topologyId);
      this.keys = MarshallableCollection.unwrap(wrappedKeys);
      this.f = MarshallableObject.unwrap(wrappedFunction);
      this.params = params;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @ProtoField(number = 3, name = "keys")
   MarshallableCollection<?> getWrappedKeys() {
      return MarshallableCollection.create(keys);
   }

   @ProtoField(number = 4,  name = "function")
   MarshallableObject<Function<ReadEntryView<K, V>, R>> getWrappedFunction() {
      return MarshallableObject.create(f);
   }

   @ProtoField(5)
   public Params getParams() {
      return params;
   }

   @ProtoField(6)
   public DataConversion getKeyDataConversion() {
      return keyDataConversion;
   }

   @ProtoField(7)
   public DataConversion getValueDataConversion() {
      return valueDataConversion;
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
   }

   public Collection<?> getKeys() {
      return keys;
   }

   public void setKeys(Collection<?> keys) {
      this.keys = keys;
   }

   public final ReadOnlyManyCommand<K, V, R> withKeys(Collection<?> keys) {
      setKeys(keys);
      return this;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadOnlyManyCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.OWNER;
   }

   public Function<ReadEntryView<K, V>, R> getFunction() {
      return f;
   }

   @Override
   public String toString() {
      return "ReadOnlyManyCommand{" + ", keys=" + keys +
            ", f=" + f.getClass().getName() +
            ", keyDataConversion=" + keyDataConversion +
            ", valueDataConversion=" + valueDataConversion +
            '}';
   }
}
