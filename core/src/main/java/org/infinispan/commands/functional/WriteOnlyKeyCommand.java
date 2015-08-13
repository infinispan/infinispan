package org.infinispan.commands.functional;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;

import java.util.Set;
import java.util.function.Consumer;

public final class WriteOnlyKeyCommand<K, V> extends AbstractWriteKeyCommand<K, V> {

   public static final byte COMMAND_ID = 54;

   private Consumer<WriteEntryView<V>> f;

   public WriteOnlyKeyCommand(K key, Consumer<WriteEntryView<V>> f, CommandInvocationId id) {
      super(key, f.getClass().getAnnotation(SerializeWith.class), id);
      this.f = f;
   }

   public WriteOnlyKeyCommand() {
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;  // TODO: Customise this generated block
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) throw new IllegalStateException("Invalid method id");
      key = parameters[0];
      f = (Consumer<WriteEntryView<V>>) parameters[1];
      valueMatcher = (ValueMatcher) parameters[2];
      flags = (Set<Flag>) parameters[3];
      commandInvocationId = (CommandInvocationId) parameters[4];
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{key, f, valueMatcher, Flag.copyWithoutRemotableFlags(flags), commandInvocationId};
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitWriteOnlyKeyCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      CacheEntry<K, V> e = ctx.lookupEntry(key);

      // Could be that the key is not local
      if (e == null) return null;

      f.accept(EntryViews.writeOnly(e));
      return null;
   }

   @Override
   public void updateStatusFromRemoteResponse(Object remoteResponse) {
      // TODO: Customise this generated block
   }

   @Override
   public boolean isWriteOnly() {
      return true;
   }

}
