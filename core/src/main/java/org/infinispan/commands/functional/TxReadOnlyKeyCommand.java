package org.infinispan.commands.functional;

import static org.infinispan.functional.impl.EntryViews.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.functional.EntryView;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;

public class TxReadOnlyKeyCommand<K, V, R> extends ReadOnlyKeyCommand<K, V, R> {
   public static final byte COMMAND_ID = 64;

   private List<Mutation<K, V, ?>> mutations;

   public TxReadOnlyKeyCommand() {
   }

   public TxReadOnlyKeyCommand(Object key, List<Mutation<K, V, ?>> mutations) {
      super(key, null, Params.create());
      this.mutations = mutations;
   }

   public TxReadOnlyKeyCommand(ReadOnlyKeyCommand other, List<Mutation<K, V, ?>> mutations) {
      super(other.getKey(), other.f, Params.create());
      this.mutations = mutations;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      super.writeTo(output);
      MarshallUtil.marshallCollection(mutations, output, Mutations::writeTo);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      super.readFrom(input);
      mutations = MarshallUtil.unmarshallCollection(input, ArrayList::new, Mutations::readFrom);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      if (mutations == null || mutations.isEmpty()) {
         return super.perform(ctx);
      }
      MVCCEntry<K, V> entry = (MVCCEntry<K, V>) ctx.lookupEntry(key);
      EntryView.ReadWriteEntryView<K, V> rw = EntryViews.readWrite(entry);
      Object ret = null;
      for (Mutation<K, V, ?> mutation : mutations) {
         ret = mutation.apply(rw);
         entry.updatePreviousValue();
      }
      if (f != null) {
         ret = f.apply(rw);
      }
      return snapshot(ret);
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("TxReadOnlyKeyCommand{");
      sb.append("key=").append(key);
      sb.append(", f=").append(f);
      sb.append(", mutations=").append(mutations);
      sb.append('}');
      return sb.toString();
   }
}
