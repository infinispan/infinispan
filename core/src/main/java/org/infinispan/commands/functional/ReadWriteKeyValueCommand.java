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
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.Param.StatisticsMode;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.EntryViews.AccessLoggingReadWriteView;
import org.infinispan.functional.impl.Params;
import org.infinispan.functional.impl.StatsEnvelope;
import org.infinispan.metadata.Metadata;

public final class ReadWriteKeyValueCommand<K, V, T, R> extends AbstractWriteKeyCommand<K, V> {
   public static final byte COMMAND_ID = 51;

   private Object argument;
   private BiFunction<T, ReadWriteEntryView<K, V>, R> f;

   public ReadWriteKeyValueCommand(Object key, Object argument, BiFunction<T, ReadWriteEntryView<K, V>, R> f,
                                   CommandInvocationId id,
                                   Params params,
                                   DataConversion keyDataConversion,
                                   DataConversion valueDataConversion,
                                   InvocationManager invocationManager,
                                   ComponentRegistry componentRegistry) {
      super(key, id, params, keyDataConversion, valueDataConversion, invocationManager);
      this.argument = argument;
      this.f = f;
      if (componentRegistry != null) {
         init(componentRegistry);
      }
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
      output.writeObject(argument);
      output.writeObject(f);
      Params.writeObject(output, params);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      CommandInvocationId.writeTo(output, commandInvocationId);
      CommandInvocationId.writeTo(output, lastInvocationId);
      DataConversion.writeTo(output, keyDataConversion);
      DataConversion.writeTo(output, valueDataConversion);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      argument = input.readObject();
      f = (BiFunction<T, ReadWriteEntryView<K, V>, R>) input.readObject();
      params = Params.readObject(input);
      setFlagsBitSet(input.readLong());
      commandInvocationId = CommandInvocationId.readFrom(input);
      lastInvocationId = CommandInvocationId.readFrom(input);
      keyDataConversion = DataConversion.readFrom(input);
      valueDataConversion = DataConversion.readFrom(input);
   }

   @Override
   public boolean isConditional() {
      return true;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      MVCCEntry<K, V> e = (MVCCEntry<K, V>) ctx.lookupEntry(key);
      Object prevValue = e.getValue();
      Metadata prevMetadata = e.getMetadata();
      T decodedArgument = (T) valueDataConversion.fromStorage(argument);
      AccessLoggingReadWriteView<K, V> view = EntryViews.readWrite(e, prevValue, prevMetadata, keyDataConversion, valueDataConversion);
      R ret = snapshot(f.apply(decodedArgument, view));
      if (e.isChanged()) {
         recordInvocation(ctx, e, prevValue, prevMetadata);
      } else {
         successful = false;
      }
      return StatisticsMode.isSkip(params) ? ret : StatsEnvelope.create(ret, e, prevValue != null, view.isRead());
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
      return new StringBuilder("ReadWriteKeyValueCommand{")
            .append("key=").append(toStr(key))
            .append(", argument=").append(toStr(argument))
            .append(", f=").append(f.getClass().getName())
            .append(", flags=").append(printFlags())
            .append(", successful=").append(successful)
            .append(", lastInvocationId=").append(lastInvocationId)
            .append(", keyDataConversion=").append(keyDataConversion)
            .append(", valueDataConversion=").append(valueDataConversion)
         .append("}")
         .toString();
   }

   @Override
   public Mutation toMutation(Object key) {
      return new Mutations.ReadWriteWithValue<>(keyDataConversion, valueDataConversion, argument, f);
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
      // Injecting the value is a way to make MarshallableFunctions$MapKey work
      InjectableComponent.inject(componentRegistry, f, argument);
   }

   public Object getArgument() {
      return argument;
   }

   public BiFunction<T, ReadWriteEntryView<K, V>, R> getBiFunction() {
      return f;
   }
}
