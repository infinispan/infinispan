package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.function.BiConsumer;

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
import org.infinispan.functional.Param.StatisticsMode;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;
import org.infinispan.functional.impl.StatsEnvelope;
import org.infinispan.marshall.MarshalledEntryUtil;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.core.UserAwareObjectOutput;

public final class WriteOnlyKeyValueCommand<K, V, T> extends AbstractWriteKeyCommand<K, V> {

   public static final byte COMMAND_ID = 55;

   private BiConsumer<T, WriteEntryView<K, V>> f;
   private Object argument;

   public WriteOnlyKeyValueCommand(Object key, Object argument,
                                   BiConsumer<T, WriteEntryView<K, V>> f,
                                   int segment, CommandInvocationId id,
                                   ValueMatcher valueMatcher,
                                   Params params,
                                   DataConversion keyDataConversion,
                                   DataConversion valueDataConversion,
                                   ComponentRegistry componentRegistry) {
      super(key, valueMatcher, segment, id, params, keyDataConversion, valueDataConversion);
      this.f = f;
      this.argument = argument;
      init(componentRegistry);
   }

   public WriteOnlyKeyValueCommand() {
      // No-op, for marshalling
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserAwareObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      output.writeKeyValue(key, argument);
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
      MarshalledEntry me = MarshalledEntryUtil.read(input);
      key = me.getKey();
      argument = me.getValue();
      f = (BiConsumer<T, WriteEntryView<K, V>>) input.readObject();
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
   public Object perform(InvocationContext ctx) throws Throwable {
      CacheEntry e = ctx.lookupEntry(key);

      // Could be that the key is not local
      if (e == null) return null;

      T decodedArgument = (T) valueDataConversion.fromStorage(argument);
      boolean exists = e.getValue() != null;
      f.accept(decodedArgument, EntryViews.writeOnly(e, valueDataConversion));
      // The effective result of retried command is not safe; we'll go to backup anyway
      if (!e.isChanged() && !hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         successful = false;
      }
      return StatisticsMode.isSkip(params) ? null : StatsEnvelope.create(null, e, exists, false);
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
   public Mutation<K, V, ?> toMutation(Object key) {
      return new Mutations.WriteWithValue<>(keyDataConversion, valueDataConversion, argument, f);
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
      if (f instanceof InjectableComponent)
         ((InjectableComponent) f).inject(componentRegistry);
   }

   public BiConsumer<T, WriteEntryView<K, V>> getBiConsumer() {
      return f;
   }

   public Object getArgument() {
      return argument;
   }
}
