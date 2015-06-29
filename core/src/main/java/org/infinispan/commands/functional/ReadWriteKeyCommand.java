package org.infinispan.commands.functional;

import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.ListenerNotifier;

import java.util.function.BiFunction;
import java.util.function.Function;

public class ReadWriteKeyCommand<K, V, R> extends AbstractDataWriteCommand {

   // TODO: Sort out when all commands have been developed
   public static final byte COMMAND_ID = 50;

   private Function<ReadWriteEntryView<K, V>, R> f;
   private ListenerNotifier<K, V> notifier;

   public ReadWriteKeyCommand(ListenerNotifier<K, V> notifier, K key,
         Function<ReadWriteEntryView<K, V>, R> f) {
      super(key, null);
      this.f = f;
      this.notifier = notifier;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      // TODO: Customise this generated block
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      MVCCEntry<K, V> e = (MVCCEntry<K, V>) ctx.lookupEntry(key);
      return f.apply(EntryViews.readWrite(e, notifier));
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
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      // TODO: Customise this generated block
   }

   @Override
   public void updateStatusFromRemoteResponse(Object remoteResponse) {
      // TODO: Customise this generated block
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadWriteKeyCommand(ctx, this);
   }
}
