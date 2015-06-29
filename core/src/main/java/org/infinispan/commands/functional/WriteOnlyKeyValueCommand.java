package org.infinispan.commands.functional;

import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.api.functional.EntryView;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.ListenerNotifier;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class WriteOnlyKeyValueCommand<K, V> extends AbstractDataWriteCommand {

   // TODO: Sort out when all commands have been developed
   public static final byte COMMAND_ID = 48;

   private BiConsumer<V, WriteEntryView<V>> f;
   private V value;
   private ValueMatcher valueMatcher;
   private ListenerNotifier<K, V> notifier;

   public WriteOnlyKeyValueCommand(ListenerNotifier<K, V> notifier, K key, V value, BiConsumer<V, WriteEntryView<V>> f) {
      super(key, null);
      this.f = f;
      this.value = value;
      this.valueMatcher = ValueMatcher.MATCH_ALWAYS;
      this.notifier = notifier;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      // TODO: Customise this generated block
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      MVCCEntry<K, V> e = (MVCCEntry<K, V>) ctx.lookupEntry(key);
      f.accept(value, EntryViews.writeOnly(e, notifier));
      return null;
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
      return false;  // TODO: Customise this generated block
   }

   @Override
   public boolean isConditional() {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return valueMatcher;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      this.valueMatcher = valueMatcher;
   }

   @Override
   public void updateStatusFromRemoteResponse(Object remoteResponse) {
      // TODO: Customise this generated block
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitWriteOnlyKeyValueCommand(ctx, this);
   }
}
