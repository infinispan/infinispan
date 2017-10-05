package org.infinispan.commands.functional;

import static org.infinispan.commons.util.Util.toStr;
import static org.infinispan.functional.impl.EntryViews.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Function;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;

// TODO: the command does not carry previous values to backup, so it can cause
// the values on primary and backup owners to diverge in case of topology change
public final class ReadWriteKeyCommand<K, V, R> extends AbstractWriteKeyCommand<K, V> {

   public static final byte COMMAND_ID = 50;

   private Function<ReadWriteEntryView<K, V>, R> f;

   public ReadWriteKeyCommand(K key, Function<ReadWriteEntryView<K, V>, R> f,
                              CommandInvocationId id, ValueMatcher valueMatcher, Params params,
                              DataConversion keyDataConversion,
                              DataConversion valueDataConversion,
                              ComponentRegistry componentRegistry) {
      super(key, valueMatcher, id, params, keyDataConversion, valueDataConversion);
      this.f = f;
      init(componentRegistry);
   }

   public ReadWriteKeyCommand() {
      // No-op, for marshalling
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
      output.writeObject(keyDataConversion);
      output.writeObject(valueDataConversion);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      f = (Function<ReadWriteEntryView<K, V>, R>) input.readObject();
      valueMatcher = MarshallUtil.unmarshallEnum(input, ValueMatcher::valueOf);
      params = Params.readObject(input);
      setFlagsBitSet(input.readLong());
      commandInvocationId = CommandInvocationId.readFrom(input);
      keyDataConversion = (DataConversion) input.readObject();
      valueDataConversion = (DataConversion) input.readObject();
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

      R ret;
      ReadWriteEntryView<K, V> entry = EntryViews.readWrite(e, keyDataConversion, valueDataConversion);
      ret = f.apply(entry);
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

   public Function<ReadWriteEntryView<K, V>, R> getFunction() {
      return f;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("ReadWriteKeyCommand{");
      sb.append(", key=").append(toStr(key));
      sb.append(", f=").append(f.getClass().getName());
      sb.append(", flags=").append(printFlags());
      sb.append(", commandInvocationId=").append(commandInvocationId);
      sb.append(", params=").append(params);
      sb.append(", valueMatcher=").append(valueMatcher);
      sb.append(", successful=").append(successful);
      sb.append(", keyDataConversion=").append(keyDataConversion);
      sb.append(", valueDataConversion=").append(valueDataConversion);
      sb.append('}');
      return sb.toString();
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
      if (f instanceof InjectableComponent)
         ((InjectableComponent) f).inject(componentRegistry);
   }
}
