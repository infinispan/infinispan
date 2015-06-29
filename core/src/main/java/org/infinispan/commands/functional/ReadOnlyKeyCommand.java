package org.infinispan.commands.functional;

import org.infinispan.commands.Visitor;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;

import java.util.function.Function;

public class ReadOnlyKeyCommand<K, V, R> extends AbstractDataCommand {

   public static final byte COMMAND_ID = 47;

   private Function<ReadEntryView<K, V>, R> f;
   private InternalCacheEntry remotelyFetchedValue;

   public ReadOnlyKeyCommand(Object key, Function<ReadEntryView<K, V>, R> f) {
      super(key, null);
      this.f = f;
   }

   public ReadOnlyKeyCommand() {
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      // No-op
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      CacheEntry<K, V> entry = ctx.lookupEntry(key);
      return perform(entry);
   }

   public Object perform(CacheEntry<K, V> entry) {
      ReadEntryView<K, V> ro = EntryViews.readOnly(entry);
      return f.apply(ro);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[0];
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