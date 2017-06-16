package org.infinispan.commands.functional;

import static org.infinispan.functional.impl.EntryViews.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.infinispan.functional.EntryView;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;

public class TxReadOnlyManyCommand<K, V, R> extends ReadOnlyManyCommand<K, V, R> {
   public static final byte COMMAND_ID = 65;
   // These mutations must have the same order of iteration as keys. We can guarantee that because the mutations
   // are set only when replicating the command to other nodes where we have already narrowed the key set
   private List<List<Mutation<K, V, ?>>> mutations;

   public TxReadOnlyManyCommand() {
   }

   public TxReadOnlyManyCommand(Collection<? extends K> keys, List<List<Mutation<K, V, ?>>> mutations) {
      super(keys, null, Params.create());
      this.mutations = mutations;
   }

   public TxReadOnlyManyCommand(ReadOnlyManyCommand c, List<List<Mutation<K, V, ?>>> mutations) {
      super(c);
      this.mutations = mutations;
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
   public Object perform(InvocationContext ctx) throws Throwable {
      if (mutations == null) {
         return super.perform(ctx);
      }
      ArrayList<R> retvals = new ArrayList<>(keys.size());
      Iterator<List<Mutation<K, V, ?>>> mutIt = mutations.iterator();
      for (K k : keys) {
         List<Mutation<K, V, ?>> mutations = mutIt.next();
         MVCCEntry<K, V> entry = (MVCCEntry<K, V>) lookupCacheEntry(ctx, k);
         EntryView.ReadEntryView<K, V> ro;
         Object ret = null;
         if (mutations.isEmpty()) {
            ro = entry.isNull() ? EntryViews.noValue(k) : EntryViews.readOnly(entry);
         } else {
            EntryView.ReadWriteEntryView rw = EntryViews.readWrite(entry);
            for (Mutation<K, V, ?> mutation : mutations) {
               ret = mutation.apply(rw);
               entry.updatePreviousValue();
            }
            ro = rw;
         }
         if (f != null) {
            ret = f.apply(ro);
         }
         retvals.add(snapshot((R) ret));
      }
      return retvals.stream();
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("TxReadOnlyManyCommand{");
      sb.append("keys=").append(keys);
      sb.append(", f=").append(f);
      sb.append(", mutations=").append(mutations);
      sb.append('}');
      return sb.toString();
   }
}
