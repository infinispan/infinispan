package org.infinispan.commands.functional;

import org.infinispan.commands.Visitor;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.lifecycle.ComponentStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public final class WriteOnlyManyEntriesCommand<K, V> extends AbstractWriteManyCommand {

   public static final byte COMMAND_ID = 54;

   private Map<? extends K, ? extends V> entries;
   private BiConsumer<V, WriteEntryView<V>> f;

   public WriteOnlyManyEntriesCommand(Map<? extends K, ? extends V> entries, BiConsumer<V, WriteEntryView<V>> f) {
      this.entries = entries;
      this.f = f;
   }

   public WriteOnlyManyEntriesCommand(WriteOnlyManyEntriesCommand<K, V> command) {
      this.entries = command.entries;
      this.f = command.f;
   }

   public WriteOnlyManyEntriesCommand() {
   }

   public Map<? extends K, ? extends V> getEntries() {
      return entries;
   }

   public void setEntries(Map<? extends K, ? extends V> entries) {
      this.entries = entries;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;  // TODO: Customise this generated block
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      entries = (Map<? extends K, ? extends V>) parameters[0];
      f = (BiConsumer<V, WriteEntryView<V>>) parameters[1];
      isForwarded = (Boolean) parameters[2];
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{entries, f, isForwarded};
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      // Can't return a lazy stream here because the current code in
      // EntryWrappingInterceptor expects any changes to be done eagerly,
      // otherwise they're not applied. So, apply the function eagerly and
      // return a lazy stream of the void returns.
      List<Void> returns = new ArrayList<>(entries.size());
      for (Map.Entry<? extends K, ? extends V> entry : entries.entrySet()) {
         CacheEntry<K, V> cacheEntry = ctx.lookupEntry(entry.getKey());

         // Could be that the key is not local, 'null' is how this is signalled
         if (cacheEntry != null) {
            f.accept(entry.getValue(), EntryViews.writeOnly(cacheEntry, null));
            returns.add(null);
         }
      }
      return returns.stream();
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public boolean canBlock() {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public Set<Object> getAffectedKeys() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void updateStatusFromRemoteResponse(Object remoteResponse) {
      // TODO: Customise this generated block
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitWriteOnlyManyEntriesCommand(ctx, this);
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;  // TODO: Customise this generated block
   }

}
