package org.infinispan.commands.functional;

import org.infinispan.commands.LocalCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;

import java.util.function.Function;

public final class ReadOnlyKeyCommand<K, V, R> extends AbstractDataCommand implements LocalCommand {

   private Function<ReadEntryView<K, V>, R> f;

   public ReadOnlyKeyCommand(Object key, Function<ReadEntryView<K, V>, R> f) {
      super(key, null);
      this.f = f;
   }

   public ReadOnlyKeyCommand() {
   }

   @Override
   public byte getCommandId() {
      return -1;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      // Not really replicated
   }

   @Override
   public Object[] getParameters() {
      return new Object[0];
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
      return f.apply(ro);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadOnlyKeyCommand(ctx, this);
   }

   @Override
   public String toString() {
      return "ReadOnlyKeyCommand{" +
            "f=" + f +
            '}';
   }

}