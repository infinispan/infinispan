package org.infinispan.commands.functional;

import org.infinispan.commands.Visitor;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.lifecycle.ComponentStatus;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public final class WriteOnlyManyEntriesCommand<K, V> extends AbstractWriteManyCommand<K, V> {

   public static final byte COMMAND_ID = 57;

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
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(entries);
      output.writeObject(f);
      output.writeBoolean(isForwarded);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      entries = (Map<? extends K, ? extends V>) input.readObject();
      f = (BiConsumer<V, WriteEntryView<V>>) input.readObject();
      isForwarded = input.readBoolean();
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      for (Map.Entry<? extends K, ? extends V> entry : entries.entrySet()) {
         CacheEntry<K, V> cacheEntry = ctx.lookupEntry(entry.getKey());

         // Could be that the key is not local, 'null' is how this is signalled
         if (cacheEntry != null)
            f.accept(entry.getValue(), EntryViews.writeOnly(cacheEntry));
      }

      return null;
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

   @Override
   public boolean readsExistingValues() {
      return false;
   }

   @Override
   public boolean alwaysReadsExistingValues() {
      return false;
   }

   @Override
   public boolean isWriteOnly() {
      return true;
   }

}
