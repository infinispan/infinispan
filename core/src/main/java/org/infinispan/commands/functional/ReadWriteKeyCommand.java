package org.infinispan.commands.functional;

import static org.infinispan.functional.impl.EntryViews.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Function;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;

// TODO: the command does not carry previous values to backup, so it can cause
// the values on primary and backup owners to diverge in case of topology change
public final class ReadWriteKeyCommand<K, V, R> extends AbstractWriteKeyCommand<K, V> {

   public static final byte COMMAND_ID = 50;

   private Function<ReadWriteEntryView<K, V>, R> f;

   public ReadWriteKeyCommand(K key, Function<ReadWriteEntryView<K, V>, R> f,
         CommandInvocationId id, ValueMatcher valueMatcher, Params params) {
      super(key, valueMatcher, id, params);
      this.f = f;
   }

   public ReadWriteKeyCommand() {
      // No-op, for marshalling
   }

   public ReadWriteKeyCommand(ReadWriteKeyCommand<K, V, R> other) {
      super((K) other.getKey(), other.getValueMatcher(), other.commandInvocationId, other.getParams());
      this.f = other.f;
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
      f = (Function<ReadWriteEntryView<K, V>, R>) input.readObject();
      valueMatcher = MarshallUtil.unmarshallEnum(input, ValueMatcher::valueOf);
      params = Params.readObject(input);
      setFlagsBitSet(input.readLong());
      commandInvocationId = CommandInvocationId.readFrom(input);
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

      R ret = f.apply(EntryViews.readWrite(e));
      return snapshot(ret);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadWriteKeyCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.OWNER;
   }

   @Override
   public Mutation<K, V, ?> toMutation(K key) {
      return new Mutations.ReadWrite<>(f);
   }
}
