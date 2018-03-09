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

public final class ReadWriteKeyCommand<K, V, R> extends AbstractWriteKeyCommand<K, V> {
   public static final byte COMMAND_ID = 50;

   private Function<ReadWriteEntryView<K, V>, R> f;

   public ReadWriteKeyCommand(Object key, Function<ReadWriteEntryView<K, V>, R> f,
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

   public ReadWriteKeyCommand() {
      // No-op, for marshalling
   }

   public ReadWriteKeyCommand(ReadWriteKeyCommand<K, V, R> other) {
      super((K) other.getKey(), other.commandInvocationId, other.getParams(),
            other.keyDataConversion, other.valueDataConversion,  other.invocationManager);
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
      DataConversion.writeTo(output, keyDataConversion);
      DataConversion.writeTo(output, valueDataConversion);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      f = (Function<ReadWriteEntryView<K, V>, R>) input.readObject();
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
      MVCCEntry e = (MVCCEntry) ctx.lookupEntry(key);
      Object prevValue = e.getValue();
      Metadata prevMetadata = e.getMetadata();
      AccessLoggingReadWriteView<K, V> view = EntryViews.readWrite(e, keyDataConversion, valueDataConversion);
      R ret = snapshot(f.apply(view));
      if (e.isChanged()) {
         recordInvocation(ctx, e, prevValue, prevMetadata);
      } else {
         successful = false;
      }
      return StatisticsMode.isSkip(params) ? ret : StatsEnvelope.create(ret, e, prevValue != null, view.isRead());
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
   public Mutation<K, V, ?> toMutation(Object key) {
      return new Mutations.ReadWrite<>(keyDataConversion, valueDataConversion, f);
   }

   public Function<ReadWriteEntryView<K, V>, R> getFunction() {
      return f;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("ReadWriteKeyCommand{");
      sb.append("key=").append(toStr(key));
      sb.append(", f=").append(f.getClass().getName());
      sb.append(", flags=").append(printFlags());
      sb.append(", commandInvocationId=").append(commandInvocationId);
      sb.append(", params=").append(params);
      sb.append(", successful=").append(successful);
      sb.append(", keyDataConversion=").append(keyDataConversion);
      sb.append(", valueDataConversion=").append(valueDataConversion);
      sb.append('}');
      return sb.toString();
   }

   @Override
   public final boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
      InjectableComponent.inject(componentRegistry, f);
   }
}
