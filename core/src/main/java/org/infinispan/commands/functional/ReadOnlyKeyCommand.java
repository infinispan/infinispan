package org.infinispan.commands.functional;

import static org.infinispan.functional.impl.EntryViews.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Function;

import org.infinispan.commands.LocalCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;

public final class ReadOnlyKeyCommand<K, V, R> extends AbstractDataCommand implements LocalCommand {

   public static final int COMMAND_ID = 62;
   private Function<ReadEntryView<K, V>, R> f;

   public ReadOnlyKeyCommand(Object key, Function<ReadEntryView<K, V>, R> f) {
      super(key, EnumUtil.EMPTY_BIT_SET);
      this.f = f;
   }

   public ReadOnlyKeyCommand() {
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeObject(f);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      f = (Function<ReadEntryView<K, V>, R>) input.readObject();
   }

   // Not really invoked unless in local mode
   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      CacheEntry<K, V> entry = ctx.lookupEntry(key);

      // Could be that the key is not local, 'null' is how this is signalled
      // When the entry is local, the entry is NullCacheEntry instead
      if (entry == null) return null;

      ReadEntryView<K, V> ro = (entry == null || entry.isNull())
         ? EntryViews.noValue((K) key) : EntryViews.readOnly(entry);
      R ret = f.apply(ro);
      return snapshot(ret);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadOnlyKeyCommand(ctx, this);
   }

   @Override
   public boolean readsExistingValues() {
      return true;
   }

   @Override
   public boolean alwaysReadsExistingValues() {
      return false;
   }

   @Override
   public String toString() {
      return "ReadOnlyKeyCommand{" +
            "key=" + key +
            "f=" + f +
            '}';
   }

}
