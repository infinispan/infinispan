package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Consumer;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.Param;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;
import org.infinispan.functional.impl.StatsEnvelope;
import org.infinispan.marshall.MarshalledEntryUtil;
import org.infinispan.marshall.core.MarshalledEntryFactory;

public final class WriteOnlyKeyCommand<K, V> extends AbstractWriteKeyCommand<K, V> {

   public static final byte COMMAND_ID = 54;

   private Consumer<WriteEntryView<K, V>> f;

   public WriteOnlyKeyCommand(Object key,
                              Consumer<WriteEntryView<K, V>> f,
                              int segment, CommandInvocationId id,
                              ValueMatcher valueMatcher,
                              Params params,
                              DataConversion keyDataConversion,
                              DataConversion valueDataConversion,
                              ComponentRegistry componentRegistry) {
      super(key, valueMatcher, segment, id, params, keyDataConversion, valueDataConversion);
      this.f = f;
      init(componentRegistry);
   }

   public WriteOnlyKeyCommand() {
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      MarshalledEntryUtil.writeKey(key, entryFactory, output);
      output.writeObject(f);
      MarshallUtil.marshallEnum(valueMatcher, output);
      UnsignedNumeric.writeUnsignedInt(output, segment);
      Params.writeObject(output, params);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      CommandInvocationId.writeTo(output, commandInvocationId);
      DataConversion.writeTo(output, keyDataConversion);
      DataConversion.writeTo(output, valueDataConversion);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = MarshalledEntryUtil.readKey(input);
      f = (Consumer<WriteEntryView<K, V>>) input.readObject();
      valueMatcher = MarshallUtil.unmarshallEnum(input, ValueMatcher::valueOf);
      segment = UnsignedNumeric.readUnsignedInt(input);
      params = Params.readObject(input);
      setFlagsBitSet(input.readLong());
      commandInvocationId = CommandInvocationId.readFrom(input);
      keyDataConversion = DataConversion.readFrom(input);
      valueDataConversion = DataConversion.readFrom(input);
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
      CacheEntry e = ctx.lookupEntry(key);

      // Could be that the key is not local
      if (e == null) return null;

      // should we leak this to stats in write-only commands? we do that for events anyway...
      boolean exists = e.getValue() != null;
      f.accept(EntryViews.writeOnly(e, valueDataConversion));
      // The effective result of retried command is not safe; we'll go to backup anyway
      if (!e.isChanged() && !hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         successful = false;
      }
      return Param.StatisticsMode.isSkip(params) ? null : StatsEnvelope.create(null, e, exists, false);
   }

   @Override
   public boolean isWriteOnly() {
      return true;
   }

   @Override
   public Mutation<K, V, ?> toMutation(Object key) {
      return new Mutations.Write(keyDataConversion, valueDataConversion, f);
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
      if (f instanceof InjectableComponent)
         ((InjectableComponent) f).inject(componentRegistry);
   }

   public Consumer<WriteEntryView<K, V>> getConsumer() {
      return f;
   }
}
