package org.infinispan.commands.functional;

import static org.infinispan.commons.util.Util.toStr;
import static org.infinispan.functional.impl.EntryViews.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.BiFunction;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public final class ReadWriteKeyValueCommand<K, V, R> extends AbstractWriteKeyCommand<K, V> {
   private static final Log log = LogFactory.getLog(ReadWriteKeyValueCommand.class);

   public static final byte COMMAND_ID = 51;

   private V value;
   private BiFunction<V, ReadWriteEntryView<K, V>, R> f;
   private V prevValue;
   private Metadata prevMetadata;

   public ReadWriteKeyValueCommand(K key, V value, BiFunction<V, ReadWriteEntryView<K, V>, R> f,
         CommandInvocationId id, ValueMatcher valueMatcher, Params params) {
      super(key, valueMatcher, id, params);
      this.value = value;
      this.f = f;
   }

   public ReadWriteKeyValueCommand(ReadWriteKeyValueCommand<K, V, R> other) {
      super((K) other.getKey(), other.getValueMatcher(), other.commandInvocationId, other.getParams());
      this.value = other.value;
      this.f = other.f;
      this.prevValue = other.prevValue;
      this.prevMetadata = other.prevMetadata;
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
      MarshallUtil.marshallEnum(valueMatcher, output);
      Params.writeObject(output, params);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeObject(prevValue);
      output.writeObject(prevMetadata);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      value = (V) input.readObject();
      f = (BiFunction<V, ReadWriteEntryView<K, V>, R>) input.readObject();
      valueMatcher = MarshallUtil.unmarshallEnum(input, ValueMatcher::valueOf);
      params = Params.readObject(input);
      setFlagsBitSet(input.readLong());
      commandInvocationId = CommandInvocationId.readFrom(input);
      prevValue = (V) input.readObject();
      prevMetadata = (Metadata) input.readObject();
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

      MVCCEntry<K, V> e = (MVCCEntry<K, V>) ctx.lookupEntry(key);

      // Could be that the key is not local
      if (e == null) return null;

      // Command only has one previous value, do not override it
      if (prevValue == null && !hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         prevValue = e.getValue();
         prevMetadata = e.getMetadata();
      }

      // Protect against outdated old value using the value matcher.
      // If the value has been update while on the retry, use the newer value.
      // Also take into account that the value might have been removed.
      // TODO: Configure equivalence function
      // TODO: this won't work properly until we store if the command was executed or not...
      Object oldPrevValue = e.getValue();
      // Note: other commands don't clone the entry as they don't carry the previous value for comparison
      // using value matcher - if other commands are retried these can apply the function multiple times.
      // Here we don't want to modify the value in context when trying what would be the outcome of the operation.
      CacheEntry<K, V> copy = e.clone();
      R ret = f.apply(value, EntryViews.readWrite(copy, prevValue, prevMetadata));
      if (valueMatcher.matches(oldPrevValue, prevValue, copy.getValue())) {
         log.tracef("Execute read-write function on previous value %s and previous metadata %s", prevValue, prevMetadata);
         e.setValue(copy.getValue());
         e.setMetadata(copy.getMetadata());
         // These are the only flags that should be changed with EntryViews.readWrite
         e.setChanged(copy.isChanged());
         e.setRemoved(copy.isRemoved());
         return snapshot(ret);
      }

      return f.apply(value, EntryViews.readWrite(e, e.getValue(), e.getMetadata()));
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
      return new StringBuilder(getClass().getSimpleName())
         .append(" {key=")
         .append(toStr(key))
         .append(", value=").append(toStr(value))
         .append(", prevValue=").append(toStr(prevValue))
         .append(", prevMetadata=").append(toStr(prevMetadata))
         .append(", flags=").append(printFlags())
         .append(", valueMatcher=").append(valueMatcher)
         .append(", successful=").append(successful)
         .append("}")
         .toString();
   }

   @Override
   public Mutation toMutation(K key) {
      return new Mutations.ReadWriteWithValue<>(value, f);
   }
}
