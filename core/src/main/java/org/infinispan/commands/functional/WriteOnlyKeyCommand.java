package org.infinispan.commands.functional;

import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.ListenerNotifier;

import java.util.function.Consumer;

public class WriteOnlyKeyCommand<K, V> extends AbstractDataWriteCommand {

   // TODO: Sort out when all commands have been developed
   public static final byte COMMAND_ID = 48;

   private Consumer<WriteEntryView<V>> f;
   private ValueMatcher valueMatcher;
   private ListenerNotifier<K, V> notifier;

   public WriteOnlyKeyCommand(ListenerNotifier<K, V> notifier, K key, Consumer<WriteEntryView<V>> f) {
      super(key, null);
      this.f = f;
      this.valueMatcher = ValueMatcher.MATCH_ALWAYS;
      this.notifier = notifier;
   }

   public WriteOnlyKeyCommand() {
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      // TODO: Customise this generated block
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      MVCCEntry<K, V> e = (MVCCEntry<K, V>) ctx.lookupEntry(key);
      f.accept(EntryViews.writeOnly(e, notifier));
      return null;
   }

   @Override
   public byte getCommandId() {
      return 0;  // TODO: Customise this generated block
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
      return visitor.visitWriteOnlyKeyCommand(ctx, this);
   }

}
