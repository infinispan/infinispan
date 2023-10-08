package org.infinispan.commands.functional;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.impl.Params;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.marshall.protostream.impl.MarshallableList;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.TX_READ_ONLY_MANY_COMMAND)
public class TxReadOnlyManyCommand<K, V, R> extends ReadOnlyManyCommand<K, V, R> {
   public static final byte COMMAND_ID = 65;

   // These mutations must have the same order of iteration as keys. We can guarantee that because the mutations
   // are set only when replicating the command to other nodes where we have already narrowed the key set
   private List<List<Mutation<K, V, ?>>> mutations;

   public TxReadOnlyManyCommand(Collection<?> keys, List<List<Mutation<K, V, ?>>> mutations,
                                Params params, DataConversion keyDataConversion,
                                DataConversion valueDataConversion) {
      super(keys, null, params, keyDataConversion, valueDataConversion);
      this.mutations = mutations;
   }

   public TxReadOnlyManyCommand(ReadOnlyManyCommand c, List<List<Mutation<K, V, ?>>> mutations) {
      super(c);
      this.mutations = mutations;
   }

   @ProtoFactory
   TxReadOnlyManyCommand(long flagsWithoutRemote, int topologyId, MarshallableCollection<?> wrappedKeys,
                         MarshallableObject<Function<EntryView.ReadEntryView<K, V>, R>> wrappedFunction,
                         Params params, DataConversion keyDataConversion, DataConversion valueDataConversion,
                         Stream<MarshallableList<Mutation<K, V, ?>>> wrappedMutations) {
      super(flagsWithoutRemote, topologyId, wrappedKeys, wrappedFunction, params, keyDataConversion, valueDataConversion);
      this.mutations = wrappedMutations == null ? null : wrappedMutations.map(MarshallableList::unwrap).collect(Collectors.toList());
   }

   @ProtoField(number = 8, name = "mutations")
   Stream<MarshallableList<Mutation<K, V, ?>>> getWrappedMutations() {
      return mutations == null ? null : mutations.stream().map(MarshallableList::create);
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      super.init(componentRegistry);
      if (mutations != null) {
         for (List<Mutation<K, V, ?>> list : mutations) {
            for (Mutation<K, V, ?> m : list) {
               m.inject(componentRegistry);
            }
         }
      }
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "TxReadOnlyManyCommand{" + "keys=" + keys +
            ", f=" + f +
            ", mutations=" + mutations +
            ", keyDataConversion=" + keyDataConversion +
            ", valueDataConversion=" + valueDataConversion +
            '}';
   }

   public List<List<Mutation<K, V, ?>>> getMutations() {
      return mutations;
   }
}
