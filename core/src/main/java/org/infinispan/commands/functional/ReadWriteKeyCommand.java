package org.infinispan.commands.functional;

import static org.infinispan.commons.util.Util.toStr;
import static org.infinispan.functional.impl.EntryViews.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Function;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationManager;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;

public final class ReadWriteKeyCommand<K, V, R> extends AbstractWriteKeyCommand<K, V> implements StrictOrderingCommand<K> {

   public static final byte COMMAND_ID = 50;
   CommandInvocationId lastInvocationId;

   private Function<ReadWriteEntryView<K, V>, R> f;

   public ReadWriteKeyCommand(K key, Function<ReadWriteEntryView<K, V>, R> f,
                              CommandInvocationId id, Params params, InvocationManager invocationManager, boolean synchronous) {
      super(key, id, params, invocationManager, synchronous);
      this.f = f;
   }

   public ReadWriteKeyCommand() {
      // No-op, for marshalling
   }

   public ReadWriteKeyCommand(ReadWriteKeyCommand<K, V, R> other) {
      super((K) other.getKey(), other.commandInvocationId, other.getParams(), other.invocationManager, other.synchronous);
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
      Params.writeObject(output, params);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      CommandInvocationId.writeTo(output, commandInvocationId);
      CommandInvocationId.writeTo(output, lastInvocationId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      f = (Function<ReadWriteEntryView<K, V>, R>) input.readObject();
      params = Params.readObject(input);
      setFlagsBitSet(input.readLong());
      commandInvocationId = CommandInvocationId.readFrom(input);
      lastInvocationId = CommandInvocationId.readFrom(input);
   }

   @Override
   public boolean isConditional() {
      return true;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      CacheEntry<K, V> e = ctx.lookupEntry(key);

      if (e == null) {
         throw new IllegalStateException();
      }

      R ret = snapshot(f.apply(EntryViews.readWrite(e)));
      recordInvocation(ctx, e, ret);
      return ret;
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

   @Override
   public CommandInvocationId getLastInvocationId(K key) {
      assert key == this.key;
      return lastInvocationId;
   }

   @Override
   public void setLastInvocationId(K key, CommandInvocationId id) {
      assert key == this.key;
      this.lastInvocationId = id;
   }

   @Override
   public String toString() {
      return new StringBuilder("ReadWriteKeyCommand{key=").append(toStr(key))
            .append(", f=").append(f)
            .append(", flags=").append(printFlags())
            .append(", successful=").append(successful)
            .append(", commandInvocationId=").append(commandInvocationId)
            .append(", lastInvocationId=").append(lastInvocationId)
            .append("}")
            .toString();
   }
}
