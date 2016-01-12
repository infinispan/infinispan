package org.infinispan.commands.functional;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.BiConsumer;

public final class WriteOnlyKeyValueCommand<K, V> extends AbstractWriteKeyCommand<K> {

   public static final byte COMMAND_ID = 55;

   private BiConsumer<V, WriteEntryView<V>> f;
   private V value;

   public WriteOnlyKeyValueCommand(K key, V value, BiConsumer<V, WriteEntryView<V>> f,
         CommandInvocationId id, ValueMatcher valueMatcher, Params params) {
      super(key, valueMatcher, id, params);
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
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeObject(value);
      output.writeObject(f);
      MarshallUtil.marshallEnum(valueMatcher, output);
      output.writeLong(Flag.copyWithoutRemotableFlags(getFlagsBitSet()));
      output.writeObject(commandInvocationId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      value = (V) input.readObject();
      f = (BiConsumer<V, WriteEntryView<V>>) input.readObject();
      valueMatcher = MarshallUtil.unmarshallEnum(input, ValueMatcher::valueOf);
      setFlagsBitSet(input.readLong());
      commandInvocationId = (CommandInvocationId) input.readObject();
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

      f.accept(value, EntryViews.writeOnly(e));
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
