package org.infinispan.commands.functional;

import java.util.function.Function;

import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;
import org.infinispan.functional.impl.StatsEnvelope;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.util.ByteString;

@ProtoTypeId(ProtoStreamTypeIds.READ_ONLY_KEY_COMMAND)
public class ReadOnlyKeyCommand<K, V, R> extends AbstractDataCommand {

   protected Function<ReadEntryView<K, V>, R> f;
   protected Params params;
   protected DataConversion keyDataConversion;
   protected DataConversion valueDataConversion;

   public ReadOnlyKeyCommand(ByteString cacheName, Object key, Function<ReadEntryView<K, V>, R> f, int segment, Params params,
                             DataConversion keyDataConversion, DataConversion valueDataConversion) {
      super(cacheName, key, segment, params.toFlagsBitSet());
      this.f = f;
      this.params = params;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @ProtoFactory
   ReadOnlyKeyCommand(ByteString cacheName, MarshallableObject<?> wrappedKey, long flagsWithoutRemote, int topologyId, int segment,
                      MarshallableObject<Function<ReadEntryView<K, V>, R>> wrappedFunction, Params params,
                      DataConversion keyDataConversion, DataConversion valueDataConversion) {
      super(cacheName, wrappedKey, flagsWithoutRemote, topologyId, segment);
      this.f = MarshallableObject.unwrap(wrappedFunction);
      this.params = params;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @ProtoField(6)
   MarshallableObject<Function<ReadEntryView<K, V>, R>> getWrappedFunction() {
      return MarshallableObject.create(f);
   }

   @ProtoField(7)
   public Params getParams() {
      return params;
   }

   @ProtoField(8)
   public DataConversion getKeyDataConversion() {
      return keyDataConversion;
   }

   @ProtoField(9)
   public DataConversion getValueDataConversion() {
      return valueDataConversion;
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
      if (f instanceof InjectableComponent)
         ((InjectableComponent) f).inject(componentRegistry);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadOnlyKeyCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.OWNER;
   }

   /**
    * Apply function on entry without any data
    */
   public Object performOnLostData() {
      return StatsEnvelope.create(f.apply(EntryViews.noValue(key, keyDataConversion)), true);
   }

   @Override
   public NodeVersion supportedSince() {
      return NodeVersion.SIXTEEN;
   }

   @Override
   public String toString() {
      return "ReadOnlyKeyCommand{" + ", key=" + key +
            ", f=" + f.getClass().getName() +
            ", keyDataConversion=" + keyDataConversion +
            ", valueDataConversion=" + valueDataConversion +
            '}';
   }

   public Function<ReadEntryView<K, V>, R> getFunction() {
      return f;
   }
}
