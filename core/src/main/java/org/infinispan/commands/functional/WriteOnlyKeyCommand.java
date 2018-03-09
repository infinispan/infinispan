package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Consumer;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationManager;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
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
import org.infinispan.metadata.Metadata;

public final class WriteOnlyKeyCommand<K, V> extends AbstractWriteKeyCommand<K, V> {

   public static final byte COMMAND_ID = 54;

   private Consumer<WriteEntryView<K, V>> f;

   public WriteOnlyKeyCommand(Object key,
                              Consumer<WriteEntryView<K, V>> f,
                              CommandInvocationId id,
                              Params params,
                              DataConversion keyDataConversion,
                              DataConversion valueDataConversion,
                              InvocationManager invocationManager,
                              ComponentRegistry componentRegistry) {
      super(key, id, params, keyDataConversion, valueDataConversion, invocationManager);
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
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
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
      f = (Consumer<WriteEntryView<K, V>>) input.readObject();
      params = Params.readObject(input);
      setFlagsBitSet(input.readLong());
      commandInvocationId = CommandInvocationId.readFrom(input);
      lastInvocationId = CommandInvocationId.readFrom(input);
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
      Object prevValue = e.getValue();
      Metadata prevMetadata = e.getMetadata();
      f.accept(EntryViews.writeOnly(e, valueDataConversion));
      if (e.isChanged()) {
         recordInvocation(ctx, e, prevValue, prevMetadata);
      } else {
         successful = false;
      }
      return Param.StatisticsMode.isSkip(params) ? null : StatsEnvelope.create(null, e, prevValue != null, false);
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
   protected void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
      InjectableComponent.inject(componentRegistry, f);
   }

   public Consumer<WriteEntryView<K, V>> getConsumer() {
      return f;
   }
}
