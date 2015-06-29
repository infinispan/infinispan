package org.infinispan.commands.functional;

import org.infinispan.commands.Visitor;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;

import java.util.Set;
import java.util.function.Function;

public class ReadOnlyManyCommand<K, V, R> extends AbstractDataCommand {

   public static final byte COMMAND_ID = 48;

   private Set<? extends K> keys;
   private Function<ReadEntryView<K, V>, R> f;

   public ReadOnlyManyCommand(Set<? extends K> keys, Function<ReadEntryView<K, V>, R> f) {
      this.keys = keys;
      this.f = f;
   }

   public ReadOnlyManyCommand() {
   }

   public Set<? extends K> getKeys() {
      return keys;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      // No-op
   }

   @Override
   public Object[] getParameters() {
      return new Object[0];
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      return keys.stream().map(k -> {
         CacheEntry<K, V> me = lookupCacheEntry(ctx, k);
         return f.apply(me == null ? EntryViews.noValue(k) : EntryViews.readOnly(me));
      });
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   private CacheEntry<K, V> lookupCacheEntry(InvocationContext ctx, Object key) {
      return ctx.lookupEntry(key);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadOnlyManyCommand(ctx, this);
   }

}
