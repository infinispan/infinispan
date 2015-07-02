package org.infinispan.commands.functional;

import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;

import java.util.function.Function;

public final class ReadWriteKeyCommand<K, V, R> extends AbstractWriteKeyCommand<K, V> {

   public static final byte COMMAND_ID = 47;

   private Function<ReadWriteEntryView<K, V>, R> f;

   public ReadWriteKeyCommand(K key, Function<ReadWriteEntryView<K, V>, R> f) {
      super(key, f.getClass().getAnnotation(SerializeWith.class));
      this.f = f;
   }

   public ReadWriteKeyCommand() {
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
      f = (Function<ReadWriteEntryView<K, V>, R>) parameters[1];
      valueMatcher = (ValueMatcher) parameters[2];
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{key, f, valueMatcher};
   }

   @Override
   public boolean isConditional() {
      return true;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      // It's not worth looking up the entry if we're never going to apply the change.
      if (valueMatcher == ValueMatcher.MATCH_NEVER) {
         successful = false;
         return null;
      }

      CacheEntry<K, V> e = ctx.lookupEntry(key);

      // Could be that the key is not local, 'null' is how this is signalled
      if (e == null) return null;

      return f.apply(EntryViews.readWrite(e, notifier));
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
