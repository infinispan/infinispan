package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;

import org.infinispan.cache.impl.CacheEncoders;
import org.infinispan.cache.impl.EncodingClasses;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;
import org.infinispan.marshall.core.EncoderRegistry;

public final class WriteOnlyManyEntriesCommand<K, V> extends AbstractWriteManyCommand<K, V> {

   public static final byte COMMAND_ID = 57;

   private Map<? extends K, ? extends V> entries;
   private BiConsumer<V, WriteEntryView<V>> f;

   public WriteOnlyManyEntriesCommand(Map<? extends K, ? extends V> entries,
                                      BiConsumer<V, WriteEntryView<V>> f,
                                      Params params,
                                      CommandInvocationId commandInvocationId,
                                      EncodingClasses encodingClasses,
                                      ComponentRegistry componentRegistry) {
      super(commandInvocationId, params, encodingClasses);
      this.entries = entries;
      this.f = f;
      init(componentRegistry);
   }

   public WriteOnlyManyEntriesCommand(WriteOnlyManyEntriesCommand<K, V> command) {
      super(command);
      this.entries = command.entries;
      this.f = command.f;
      this.encodingClasses = command.encodingClasses;
      this.cacheEncoders = command.cacheEncoders;
   }

   public WriteOnlyManyEntriesCommand() {
   }

   @Inject
   public void injectDependencies(EncoderRegistry encoderRegistry) {
      cacheEncoders = CacheEncoders.grabEncodersFromRegistry(encoderRegistry, encodingClasses);
   }

   public Map<? extends K, ? extends V> getEntries() {
      return entries;
   }

   public void setEntries(Map<? extends K, ? extends V> entries) {
      this.entries = entries;
   }

   public final WriteOnlyManyEntriesCommand<K, V> withEntries(Map<? extends K, ? extends V> entries) {
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
      output.writeObject(entries);
      output.writeObject(f);
      output.writeBoolean(isForwarded);
      Params.writeObject(output, params);
      output.writeInt(topologyId);
      output.writeLong(flags);
      EncodingClasses.writeTo(output, encodingClasses);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      entries = (Map<? extends K, ? extends V>) input.readObject();
      f = (BiConsumer<V, WriteEntryView<V>>) input.readObject();
      isForwarded = input.readBoolean();
      params = Params.readObject(input);
      topologyId = input.readInt();
      flags = input.readLong();
      encodingClasses = EncodingClasses.readFrom(input);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      for (Map.Entry<? extends K, ? extends V> entry : entries.entrySet()) {
         CacheEntry<K, V> cacheEntry = ctx.lookupEntry(entry.getKey());
         // Could be that the key is not local, 'null' is how this is signalled
         if (cacheEntry == null) {
            throw new IllegalStateException();
         }
         f.accept(entry.getValue(), EntryViews.writeOnly(cacheEntry, cacheEncoders));
      }
      return null;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public Collection<?> getAffectedKeys() {
      return entries.keySet();
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitWriteOnlyManyEntriesCommand(ctx, this);
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
   public String toString() {
      final StringBuilder sb = new StringBuilder("WriteOnlyManyEntriesCommand{");
      sb.append("entries=").append(entries);
      sb.append(", f=").append(f.getClass().getName());
      sb.append(", isForwarded=").append(isForwarded);
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
      V valueFromStorage = (V) cacheEncoders.valueFromStorage(entries.get(key));
      return new Mutations.WriteWithValue<>(valueFromStorage, f);
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      if (encodingClasses != null) {
         componentRegistry.wireDependencies(this);
      }
      if (f instanceof InjectableComponent)
         ((InjectableComponent) f).inject(componentRegistry);
   }
}
