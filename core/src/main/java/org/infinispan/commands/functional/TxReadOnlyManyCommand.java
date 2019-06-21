package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.impl.Params;

public class TxReadOnlyManyCommand<K, V, R> extends ReadOnlyManyCommand<K, V, R> {
   public static final byte COMMAND_ID = 65;
   // These mutations must have the same order of iteration as keys. We can guarantee that because the mutations
   // are set only when replicating the command to other nodes where we have already narrowed the key set
   private List<List<Mutation<K, V, ?>>> mutations;

   public TxReadOnlyManyCommand() {
   }

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

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      super.init(componentRegistry, isRemote);
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
   public void writeTo(ObjectOutput output) throws IOException {
      super.writeTo(output);
      // TODO: if the marshaller does not support object counting we could marshall the same functions many times
      // This encoding is optimized for mostly-empty inner lists but is as efficient as regular collection
      // encoding from MarshallUtil if all the inner lists are non-empty
      int emptyLists = 0;
      for (List<Mutation<K, V, ?>> list : mutations) {
         if (list.isEmpty()) {
            emptyLists++;
         } else {
            if (emptyLists > 0) output.writeInt(-emptyLists);
            output.writeInt(list.size());
            for (Mutation<K, V, ?> mut : list) {
               Mutations.writeTo(output, mut);
            }
            emptyLists = 0;
         }
      }
      if (emptyLists > 0) output.writeInt(-emptyLists);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      super.readFrom(input);
      int numMutations = keys.size();
      mutations = new ArrayList<>(numMutations);
      for (int i = 0; i < numMutations; ++i) {
         int length = input.readInt();
         if (length < 0) {
            i -= length;
            while (length < 0) {
               mutations.add(Collections.emptyList());
               ++length;
            }
            if (i >= numMutations) {
               break;
            }
            length = input.readInt();
         }
         List<Mutation<K, V, ?>> list = new ArrayList<>(length);
         for (int j = 0; j < length; ++j) {
            list.add(Mutations.readFrom(input));
         }
         mutations.add(list);
      }
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("TxReadOnlyManyCommand{");
      sb.append("keys=").append(keys);
      sb.append(", f=").append(f);
      sb.append(", mutations=").append(mutations);
      sb.append(", keyDataConversion=").append(keyDataConversion);
      sb.append(", valueDataConversion=").append(valueDataConversion);
      sb.append('}');
      return sb.toString();
   }

   public List<List<Mutation<K, V, ?>>> getMutations() {
      return mutations;
   }
}
