package org.infinispan.commands.functional;

import java.util.List;
import java.util.function.Function;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.impl.Params;
import org.infinispan.marshall.protostream.impl.MarshallableList;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.TX_READ_ONLY_KEY_COMMAND)
public class TxReadOnlyKeyCommand<K, V, R> extends ReadOnlyKeyCommand<K, V, R> {
   public static final byte COMMAND_ID = 64;

   private List<Mutation<K, V, ?>> mutations;

   public TxReadOnlyKeyCommand(Object key, Function<EntryView.ReadEntryView<K, V>, R> f, List<Mutation<K, V, ?>> mutations,
                               int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      super(key, f, segment, params, keyDataConversion, valueDataConversion);
      this.mutations = mutations;
   }

   @ProtoFactory
   TxReadOnlyKeyCommand(MarshallableObject<?> wrappedKey, long flagsWithoutRemote, int topologyId, int segment,
                        MarshallableObject<Function<EntryView.ReadEntryView<K, V>, R>> wrappedFunction, Params params,
                        DataConversion keyDataConversion, DataConversion valueDataConversion,
                        MarshallableList<Mutation<K, V, ?>> wrappedMutations) {
      super(wrappedKey, flagsWithoutRemote, topologyId, segment, wrappedFunction, params, keyDataConversion, valueDataConversion);
      this.mutations = MarshallableList.unwrap(wrappedMutations);
   }

   @ProtoField(number = 9, name = "mutations")
   MarshallableList<Mutation<K, V, ?>> getWrappedMutations() {
      return MarshallableList.create(mutations);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      super.init(componentRegistry);
      // This may be called from parent's constructor when mutations are not initialized yet
      List<Mutation<K, V, ?>> mutations = getMutations();
      if (mutations != null) {
         for (Mutation<?, ?, ?> m : mutations) {
            m.inject(componentRegistry);
         }
      }
   }

   @Override
   public String toString() {
      return "TxReadOnlyKeyCommand{" + "key=" + key +
            ", f=" + f +
            ", mutations=" + mutations +
            ", params=" + params +
            ", keyDataConversion=" + keyDataConversion +
            ", valueDataConversion=" + valueDataConversion +
            '}';
   }

   public List<Mutation<K, V, ?>> getMutations() {
      return mutations;
   }
}
