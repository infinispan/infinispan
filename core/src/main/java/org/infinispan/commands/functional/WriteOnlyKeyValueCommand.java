package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.BiConsumer;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;

public final class WriteOnlyKeyValueCommand<K, V> extends AbstractWriteKeyCommand<K, V> {

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
      Params.writeObject(output, params);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      CommandInvocationId.writeTo(output, commandInvocationId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      value = (V) input.readObject();
      f = (BiConsumer<V, WriteEntryView<V>>) input.readObject();
      valueMatcher = MarshallUtil.unmarshallEnum(input, ValueMatcher::valueOf);
      params = Params.readObject(input);
      setFlagsBitSet(input.readLong());
      commandInvocationId = CommandInvocationId.readFrom(input);
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
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitWriteOnlyKeyValueCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.DONT_LOAD;
   }

   @Override
   public boolean isWriteOnly() {
      return true;
   }

   @Override
   public Mutation<K, V, ?> toMutation(K key) {
      return new Mutations.WriteWithValue<>(value, f);
   }
}
