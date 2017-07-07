package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Consumer;

import org.infinispan.cache.impl.CacheEncoders;
import org.infinispan.cache.impl.EncodingClasses;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;
import org.infinispan.marshall.core.EncoderRegistry;

public final class WriteOnlyManyCommand<K, V> extends AbstractWriteManyCommand<K, V> {

   public static final byte COMMAND_ID = 56;

   private Collection<? extends K> keys;
   private Consumer<WriteEntryView<V>> f;

   public WriteOnlyManyCommand(Collection<? extends K> keys,
                               Consumer<WriteEntryView<V>> f,
                               Params params,
                               CommandInvocationId commandInvocationId,
                               EncodingClasses encodingClasses,
                               ComponentRegistry componentRegistry) {
      super(commandInvocationId, params, encodingClasses);
      this.keys = keys;
      this.f = f;
      init(componentRegistry);
   }

   public WriteOnlyManyCommand(WriteOnlyManyCommand<K, V> command) {
      super(command);
      this.keys = command.keys;
      this.f = command.f;
      this.encodingClasses = command.encodingClasses;
      this.cacheEncoders = command.cacheEncoders;
   }

   @Inject
   public void injectDependencies(EncoderRegistry encoderRegistry) {
      cacheEncoders = CacheEncoders.grabEncodersFromRegistry(encoderRegistry, encodingClasses);
   }

   public WriteOnlyManyCommand() {
   }

   public void setKeys(Collection<? extends K> keys) {
      this.keys = keys;
   }

   public final WriteOnlyManyCommand<K, V> withKeys(Collection<? extends K> keys) {
      setKeys(keys);
      return this;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      MarshallUtil.marshallCollection(keys, output);
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
      keys = MarshallUtil.unmarshallCollectionUnbounded(input, ArrayList::new);
      f = (Consumer<WriteEntryView<V>>) input.readObject();
      isForwarded = input.readBoolean();
      params = Params.readObject(input);
      topologyId = input.readInt();
      flags = input.readLong();
      encodingClasses = EncodingClasses.readFrom(input);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitWriteOnlyManyCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      for (K k : keys) {
         CacheEntry<K, V> cacheEntry = ctx.lookupEntry(k);
         if (cacheEntry == null) {
            throw new IllegalStateException();
         }
         f.accept(EntryViews.writeOnly(cacheEntry, cacheEncoders));
      }
      return null;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public Collection<?> getAffectedKeys() {
      return keys;
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
      final StringBuilder sb = new StringBuilder("WriteOnlyManyCommand{");
      sb.append("keys=").append(keys);
      sb.append(", f=").append(f.getClass().getName());
      sb.append(", isForwarded=").append(isForwarded);
      sb.append('}');
      return sb.toString();
   }

   @Override
   public Collection<Object> getKeysToLock() {
      // TODO: fixup the generics
      return (Collection<Object>) keys;
   }

   @Override
   public Mutation<K, V, ?> toMutation(K key) {
      return new Mutations.Write<>(f);
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
