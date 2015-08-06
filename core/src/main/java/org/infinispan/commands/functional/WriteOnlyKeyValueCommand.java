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
import java.util.function.BiConsumer;

public final class WriteOnlyKeyValueCommand<K, V> extends AbstractWriteKeyCommand<K, V> {

   public static final byte COMMAND_ID = 55;

   private BiConsumer<V, WriteEntryView<V>> f;
   private V value;

   public WriteOnlyKeyValueCommand(K key, V value, BiConsumer<V, WriteEntryView<V>> f, CommandInvocationId id) {
      super(key, f.getClass().getAnnotation(SerializeWith.class), id);
      this.f = f;
      this.value = value;
   }

   public WriteOnlyKeyValueCommand() {
      // No-op, for marshalling
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) throw new IllegalStateException("Invalid method id");
      key = parameters[0];
      value = (V) parameters[1];
      f = (BiConsumer<V, WriteEntryView<V>>) parameters[2];
      valueMatcher = (ValueMatcher) parameters[3];
      flags = (Set<Flag>) parameters[4];
      commandInvocationId = (CommandInvocationId) parameters[5];
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{key, value, f, valueMatcher, Flag.copyWithoutRemotableFlags(flags), commandInvocationId};
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      CacheEntry<K, V> e = ctx.lookupEntry(key);

      // Could be that the key is not local
      if (e == null) return null;

      f.accept(value, EntryViews.writeOnly(e, notifier));
      return null;
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
