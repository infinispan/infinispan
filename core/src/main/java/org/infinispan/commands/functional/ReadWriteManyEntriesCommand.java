package org.infinispan.commands.functional;

import static org.infinispan.functional.impl.EntryViews.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.Param;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.EntryViews.AccessLoggingReadWriteView;
import org.infinispan.functional.impl.Params;
import org.infinispan.functional.impl.StatsEnvelope;

// TODO: the command does not carry previous values to backup, so it can cause
// the values on primary and backup owners to diverge in case of topology change
public final class ReadWriteManyEntriesCommand<K, V, T, R> extends AbstractWriteManyCommand<K, V> {

   public static final byte COMMAND_ID = 53;

   private Map<?, ?> arguments;
   private BiFunction<T, ReadWriteEntryView<K, V>, R> f;

   boolean isForwarded = false;

   public ReadWriteManyEntriesCommand(Map<?, ?> arguments,
                                      BiFunction<T, ReadWriteEntryView<K, V>, R> f,
                                      Params params,
                                      CommandInvocationId commandInvocationId,
                                      DataConversion keyDataConversion,
                                      DataConversion valueDataConversion,
                                      ComponentRegistry componentRegistry) {
      super(commandInvocationId, params, keyDataConversion, valueDataConversion);
      this.arguments = arguments;
      this.f = f;
      init(componentRegistry);
   }

   public ReadWriteManyEntriesCommand(ReadWriteManyEntriesCommand command) {
      super(command);
      this.arguments = command.arguments;
      this.f = command.f;
      this.keyDataConversion = command.getKeyDataConversion();
      this.valueDataConversion = command.getValueDataConversion();
   }

   public ReadWriteManyEntriesCommand() {
   }

   public BiFunction<T, ReadWriteEntryView<K, V>, R> getBiFunction() {
      return f;
   }

   public Map<?, ?> getArguments() {
      return arguments;
   }

   public void setArguments(Map<?, ?> arguments) {
      this.arguments = arguments;
   }

   public final ReadWriteManyEntriesCommand<K, V, T, R> withArguments(Map<?, ?> entries) {
      setArguments(entries);
      return this;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      MarshallUtil.marshallMap(arguments, output);
      output.writeObject(f);
      output.writeBoolean(isForwarded);
      Params.writeObject(output, params);
      output.writeInt(topologyId);
      output.writeLong(flags);
      DataConversion.writeTo(output, keyDataConversion);
      DataConversion.writeTo(output, valueDataConversion);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      // We use LinkedHashMap in order to guarantee the same order of iteration
      arguments = MarshallUtil.unmarshallMap(input, LinkedHashMap::new);
      f = (BiFunction<T, ReadWriteEntryView<K, V>, R>) input.readObject();
      isForwarded = input.readBoolean();
      params = Params.readObject(input);
      topologyId = input.readInt();
      flags = input.readLong();
      keyDataConversion = DataConversion.readFrom(input);
      valueDataConversion = DataConversion.readFrom(input);
   }

   public boolean isForwarded() {
      return isForwarded;
   }

   public void setForwarded(boolean forwarded) {
      isForwarded = forwarded;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadWriteManyEntriesCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      List<Object> returns = new ArrayList<>(arguments.size());
      boolean skipStats = Param.StatisticsMode.isSkip(params);
      arguments.forEach((k, arg) -> {
         MVCCEntry entry = (MVCCEntry) ctx.lookupEntry(k);

         if (entry == null) {
            throw new IllegalStateException();
         }
         T decodedArgument = (T) valueDataConversion.fromStorage(arg);
         boolean exists = entry.getValue() != null;
         AccessLoggingReadWriteView<K, V> view = EntryViews.readWrite(entry, keyDataConversion, valueDataConversion);
         R r = snapshot(f.apply(decodedArgument, view));
         returns.add(skipStats ? r : StatsEnvelope.create(r, entry, exists, view.isRead()));
      });
      return returns;
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public Collection<?> getAffectedKeys() {
      return arguments.keySet();
   }

   public LoadType loadType() {
      return LoadType.OWNER;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("ReadWriteManyEntriesCommand{");
      sb.append("arguments=").append(arguments);
      sb.append(", f=").append(f.getClass().getName());
      sb.append(", isForwarded=").append(isForwarded);
      sb.append(", keyDataConversion=").append(keyDataConversion);
      sb.append(", valueDataConversion=").append(valueDataConversion);
      sb.append('}');
      return sb.toString();
   }

   @Override
   public Collection<?> getKeysToLock() {
      return arguments.keySet();
   }

   @Override
   public Mutation<K, V, ?> toMutation(Object key) {
      return new Mutations.ReadWriteWithValue(keyDataConversion, valueDataConversion, arguments.get(key), f);
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);

      if (f instanceof InjectableComponent)
         ((InjectableComponent) f).inject(componentRegistry);
   }

}
