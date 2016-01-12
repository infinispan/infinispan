package org.infinispan.commands.functional;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.BiFunction;

import static org.infinispan.commons.util.Util.toStr;
import static org.infinispan.functional.impl.EntryViews.snapshot;

public final class ReadWriteKeyValueCommand<K, V, R> extends AbstractWriteKeyCommand<K> {
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
      output.writeLong(Flag.copyWithoutRemotableFlags(getFlagsBitSet()));
      output.writeObject(commandInvocationId);
      output.writeObject(prevValue);
      output.writeObject(prevMetadata);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      value = (V) input.readObject();
      f = (BiFunction<V, ReadWriteEntryView<K, V>, R>) input.readObject();
      valueMatcher = MarshallUtil.unmarshallEnum(input, ValueMatcher::valueOf);
      setFlagsBitSet(input.readLong());
      commandInvocationId = (CommandInvocationId) input.readObject();
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
      if (prevValue == null && !hasFlag(Flag.COMMAND_RETRY)) {
         prevValue = e.getValue();
         prevMetadata = e.getMetadata();
      }

      // Protect against outdated old value using the value matcher.
      // If the value has been update while on the retry, use the newer value.
      // Also take into account that the value might have been removed.
      // TODO: Configure equivalence function
      if (valueUnchanged(e, prevValue, value) || valueRemoved(e, prevValue)) {
         log.tracef("Execute read-write function on previous value %s and previous metadata %s", prevValue, prevMetadata);
         R ret = f.apply(value, EntryViews.readWrite(e, prevValue, prevMetadata));
         return snapshot(ret);
      }

      return f.apply(value, EntryViews.readWrite(e, e.getValue(), e.getMetadata()));
   }


   boolean valueRemoved(MVCCEntry<K, V> e, V prevValue) {
      return valueUnchanged(e, prevValue, null);
   }

   boolean valueUnchanged(MVCCEntry<K, V> e, V prevValue, V value) {
      return valueMatcher.matches(e, prevValue, value, AnyEquivalence.getInstance());
   }

   @Override
   public void updateStatusFromRemoteResponse(Object remoteResponse) {
      // TODO: Customise this generated block
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadWriteKeyValueCommand(ctx, this);
   }

   @Override
   public boolean readsExistingValues() {
      return true;
   }

   @Override
   public boolean alwaysReadsExistingValues() {
      return false;
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

}
