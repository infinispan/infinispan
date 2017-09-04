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
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;

// TODO: the command does not carry previous values to backup, so it can cause
// the values on primary and backup owners to diverge in case of topology change
public final class ReadWriteManyEntriesCommand<K, V, R> extends AbstractWriteManyCommand<K, V> {

   public static final byte COMMAND_ID = 53;

   private Map<? extends K, ? extends V> entries;
   private BiFunction<V, ReadWriteEntryView<K, V>, R> f;

   boolean isForwarded = false;

   public ReadWriteManyEntriesCommand(Map<? extends K, ? extends V> entries,
                                      BiFunction<V, ReadWriteEntryView<K, V>, R> f,
                                      Params params,
                                      CommandInvocationId commandInvocationId,
                                      DataConversion keyDataConversion,
                                      DataConversion valueDataConversion,
                                      ComponentRegistry componentRegistry) {
      super(commandInvocationId, params, keyDataConversion, valueDataConversion);
      this.entries = entries;
      this.f = f;
      init(componentRegistry);
   }

   public ReadWriteManyEntriesCommand(ReadWriteManyEntriesCommand command) {
      super(command);
      this.entries = command.entries;
      this.f = command.f;
      this.keyDataConversion = command.getKeyDataConversion();
      this.valueDataConversion = command.getValueDataConversion();
   }

   public ReadWriteManyEntriesCommand() {
   }

   public Map<? extends K, ? extends V> getEntries() {
      return entries;
   }

   public void setEntries(Map<? extends K, ? extends V> entries) {
      this.entries = entries;
   }

   public final ReadWriteManyEntriesCommand<K, V, R> withEntries(Map<? extends K, ? extends V> entries) {
      setEntries(entries);
      return this;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      MarshallUtil.marshallMap(entries, output);
      output.writeObject(f);
      output.writeBoolean(isForwarded);
      Params.writeObject(output, params);
      output.writeInt(topologyId);
      output.writeLong(flags);
      output.writeObject(keyDataConversion);
      output.writeObject(valueDataConversion);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      // We use LinkedHashMap in order to guarantee the same order of iteration
      entries = MarshallUtil.unmarshallMap(input, LinkedHashMap::new);
      f = (BiFunction<V, ReadWriteEntryView<K, V>, R>) input.readObject();
      isForwarded = input.readBoolean();
      params = Params.readObject(input);
      topologyId = input.readInt();
      flags = input.readLong();
      keyDataConversion = (DataConversion) input.readObject();
      valueDataConversion = (DataConversion) input.readObject();
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
      List<R> returns = new ArrayList<>(entries.size());
      entries.forEach((k, v) -> {
         CacheEntry<K, V> entry = ctx.lookupEntry(k);

         if (entry == null) {
            throw new IllegalStateException();
         }
         V decodedValue = (V) valueDataConversion.fromStorage(v);
         R r = f.apply(decodedValue, EntryViews.readWrite(entry, keyDataConversion, valueDataConversion));
         returns.add(snapshot(r));
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
      return entries.keySet();
   }

   public LoadType loadType() {
      return LoadType.OWNER;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("ReadWriteManyEntriesCommand{");
      sb.append("entries=").append(entries);
      sb.append(", f=").append(f.getClass().getName());
      sb.append(", isForwarded=").append(isForwarded);
      sb.append(", keyDataConversion=").append(keyDataConversion);
      sb.append(", valueDataConversion=").append(valueDataConversion);
      sb.append('}');
      return sb.toString();
   }

   @Override
   public Collection<Object> getKeysToLock() {
      // TODO: fixup the generics
      return (Collection<Object>) entries.keySet();
   }

   @Override
   public Mutation<K, V, ?> toMutation(K key) {
      V valueFromStorage = (V) valueDataConversion.fromStorage(entries.get(key));
      return new Mutations.ReadWriteWithValue(valueFromStorage, f);
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);

      if (f instanceof InjectableComponent)
         ((InjectableComponent) f).inject(componentRegistry);
   }
}
