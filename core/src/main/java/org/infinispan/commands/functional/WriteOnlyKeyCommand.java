package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Consumer;

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

public final class WriteOnlyKeyCommand<K, V> extends AbstractWriteKeyCommand<K, V> {

   public static final byte COMMAND_ID = 54;

   private Consumer<WriteEntryView<V>> f;

   public WriteOnlyKeyCommand(K key, Consumer<WriteEntryView<V>> f,
         CommandInvocationId id, ValueMatcher valueMatcher, Params params) {
      super(key, valueMatcher, id, params);
      this.f = f;
   }

   public WriteOnlyKeyCommand() {
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeObject(f);
      MarshallUtil.marshallEnum(valueMatcher, output);
      Params.writeObject(output, params);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      CommandInvocationId.writeTo(output, commandInvocationId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      f = (Consumer<WriteEntryView<V>>) input.readObject();
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
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitWriteOnlyKeyCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.DONT_LOAD;
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
   public boolean isWriteOnly() {
      return true;
   }

   @Override
   public Mutation<K, V, ?> toMutation(K key) {
      return new Mutations.Write(f);
   }
}
