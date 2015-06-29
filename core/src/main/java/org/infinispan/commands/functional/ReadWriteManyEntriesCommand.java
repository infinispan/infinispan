package org.infinispan.commands.functional;

import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.api.functional.EntryView;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.ListenerNotifier;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.metadata.Metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ReadWriteManyEntriesCommand<K, V, R> implements WriteCommand {

   // TODO: Sort out when all commands have been developed
   public static final byte COMMAND_ID = 48;

   private ListenerNotifier<K, V> notifier;
   private Map<? extends K, ? extends V> entries;
   private BiFunction<V, ReadWriteEntryView<K, V>, R> f;

   public ReadWriteManyEntriesCommand(ListenerNotifier<K, V> notifier,
         Map<? extends K, ? extends V> entries, BiFunction<V, ReadWriteEntryView<K, V>, R> f) {
      this.entries = entries;
      this.f = f;
      this.notifier = notifier;
   }

   public ReadWriteManyEntriesCommand() {
   }

   public Map<? extends K, ? extends V> getEntries() {
      return entries;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      // TODO: Customise this generated block
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
   public Object perform(InvocationContext ctx) throws Throwable {
      // Can't return a lazy stream here because the current code in
      // EntryWrappingInterceptor expects any changes to be done eagerly,
      // otherwise they're not applied. So, apply the function eagerly and
      // return a lazy stream of the void returns.
      List<R> returns = new ArrayList<>(entries.size());
      entries.forEach((k, v) -> {
         CacheEntry<K, V> entry = ctx.lookupEntry(k);
         R r = f.apply(v, EntryViews.readWrite(entry, notifier));
         returns.add(r);
      });
      return returns.stream();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[0];  // TODO: Customise this generated block
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return ValueMatcher.MATCH_ALWAYS;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      // No-op
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
      return visitor.visitReadWriteManyEntriesCommand(ctx, this);
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public Set<Flag> getFlags() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void setFlags(Set<Flag> flags) {
      // TODO: Customise this generated block
   }

   @Override
   public void setFlags(Flag... flags) {
      // TODO: Customise this generated block
   }

   @Override
   public boolean hasFlag(Flag flag) {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public Metadata getMetadata() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void setMetadata(Metadata metadata) {
      // TODO: Customise this generated block
   }

   @Override
   public int getTopologyId() {
      return 0;  // TODO: Customise this generated block
   }

   @Override
   public void setTopologyId(int topologyId) {
      // TODO: Customise this generated block
   }

}
