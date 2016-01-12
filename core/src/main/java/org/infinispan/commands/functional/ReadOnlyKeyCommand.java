package org.infinispan.commands.functional;

import org.infinispan.commands.LocalCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Function;

import static org.infinispan.functional.impl.EntryViews.snapshot;

public final class ReadOnlyKeyCommand<K, V, R> extends AbstractDataCommand implements LocalCommand {

   private Function<ReadEntryView<K, V>, R> f;

   public ReadOnlyKeyCommand(Object key, Function<ReadEntryView<K, V>, R> f) {
      super(key, EnumUtil.EMPTY_BIT_SET);
      this.f = f;
   }

   public ReadOnlyKeyCommand() {
   }

   @Override
   public byte getCommandId() {
      return -1;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      // Not really replicated
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      // Not really replicated
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      CacheEntry<K, V> entry = ctx.lookupEntry(key);

      // Could be that the key is not local, 'null' is how this is signalled
      if (entry == null) return null;

      return perform(entry);
   }

   public Object perform(CacheEntry<K, V> entry) {
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
            "f=" + f +
            '}';
   }

}
