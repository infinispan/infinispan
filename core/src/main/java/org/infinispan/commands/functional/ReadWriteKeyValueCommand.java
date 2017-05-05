package org.infinispan.commands.functional;

import static org.infinispan.commons.util.Util.toStr;
import static org.infinispan.functional.impl.EntryViews.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.BiFunction;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationManager;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public final class ReadWriteKeyValueCommand<K, V, R> extends AbstractWriteKeyCommand<K, V> implements StrictOrderingCommand<K> {
   private static final Log log = LogFactory.getLog(ReadWriteKeyValueCommand.class);

   public static final byte COMMAND_ID = 51;
   CommandInvocationId lastInvocationId;

   private V value;
   private BiFunction<V, ReadWriteEntryView<K, V>, R> f;

   public ReadWriteKeyValueCommand(K key, V value, BiFunction<V, ReadWriteEntryView<K, V>, R> f,
                                   CommandInvocationId id, Params params, InvocationManager invocationManager, boolean synchronous) {
      super(key, id, params, invocationManager, synchronous);
      this.value = value;
      this.f = f;
   }

   public ReadWriteKeyValueCommand(ReadWriteKeyValueCommand<K, V, R> other) {
      super((K) other.getKey(), other.commandInvocationId, other.getParams(), other.invocationManager, other.synchronous);
      this.value = other.value;
      this.f = other.f;
   }

   public ReadWriteKeyValueCommand() {
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
      Params.writeObject(output, params);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      CommandInvocationId.writeTo(output, commandInvocationId);
      CommandInvocationId.writeTo(output, lastInvocationId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      value = (V) input.readObject();
      f = (BiFunction<V, ReadWriteEntryView<K, V>, R>) input.readObject();
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
      MVCCEntry<K, V> e = (MVCCEntry<K, V>) ctx.lookupEntry(key);

      if (e == null) {
         throw new IllegalStateException();
      }

      R ret = snapshot(f.apply(value, EntryViews.readWrite(e)));
      recordInvocation(ctx, e, ret);
      return ret;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadWriteKeyValueCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.OWNER;
   }

   @Override
   public String toString() {
      return new StringBuilder("ReadWriteKeyValueCommand{key=").append(toStr(key))
            .append(", value=").append(toStr(value))
            .append(", f=").append(f)
            .append(", flags=").append(printFlags())
            .append(", successful=").append(successful)
            .append(", commandInvocationId=").append(commandInvocationId)
            .append(", lastInvocationId=").append(lastInvocationId)
            .append("}")
            .toString();
   }

   @Override
   public Mutation toMutation(K key) {
      return new Mutations.ReadWriteWithValue<>(value, f);
   }

   @Override
   public CommandInvocationId getLastInvocationId(K key) {
      assert key == this.key;
      return lastInvocationId == CommandInvocationId.DUMMY_INVOCATION_ID ? null : lastInvocationId;
   }

   @Override
   public void setLastInvocationId(K key, CommandInvocationId id) {
      assert key == this.key;
      this.lastInvocationId = id;
   }
}
