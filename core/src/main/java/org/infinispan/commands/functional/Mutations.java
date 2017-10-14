package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.core.EncoderRegistry;

/**
 * Helper class for marshalling, also hiding implementations of {@link Mutation} from the interface.
 */
final class Mutations {
   private Mutations() {}

   // No need to occupy externalizer ids when we have a limited set of options
   static <K, V, R> void writeTo(ObjectOutput output, Mutation<K, V, R> mutation) throws IOException {
      BaseMutation bm = (BaseMutation) mutation;
      DataConversion.writeTo(output, bm.keyDataConversion);
      DataConversion.writeTo(output, bm.valueDataConversion);
      byte type = mutation.type();
      output.writeByte(type);
      switch (type) {
         case ReadWrite.TYPE:
            output.writeObject(((ReadWrite<K, V, ?>) mutation).f);
            break;
         case ReadWriteWithValue.TYPE:
            ReadWriteWithValue<K, V, R> rwwv = (ReadWriteWithValue<K, V, R>) mutation;
            output.writeObject(rwwv.value);
            output.writeObject(rwwv.f);
            break;
         case Write.TYPE:
            output.writeObject(((Write<K, V>) mutation).f);
            break;
         case WriteWithValue.TYPE:
            WriteWithValue<K, V> wwv = (WriteWithValue<K, V>) mutation;
            output.writeObject(wwv.value);
            output.writeObject(wwv.f);
            break;
      }
   }

   static <K, V> Mutation<K, V, ?> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      DataConversion keyDataConversion = DataConversion.readFrom(input);
      DataConversion valueDataConversion = DataConversion.readFrom(input);
      switch (input.readByte()) {
         case ReadWrite.TYPE:
            return new ReadWrite<>(keyDataConversion, valueDataConversion, (Function<EntryView.ReadWriteEntryView<K, V>, ?>) input.readObject());
         case ReadWriteWithValue.TYPE:
            return new ReadWriteWithValue<>(keyDataConversion, valueDataConversion, input.readObject(), (BiFunction<V, EntryView.ReadWriteEntryView<K, V>, ?>) input.readObject());
         case Write.TYPE:
            return new Write<>(keyDataConversion, valueDataConversion, (Consumer<EntryView.WriteEntryView<V>>) input.readObject());
         case WriteWithValue.TYPE:
            return new WriteWithValue<>(keyDataConversion, valueDataConversion, input.readObject(), (BiConsumer<V, EntryView.WriteEntryView<V>>) input.readObject());
         default:
            throw new IllegalStateException("Unknown type of mutation, broken input?");
      }
   }

   static abstract class BaseMutation<K, V, R> implements Mutation<K, V, R> {
      protected final DataConversion keyDataConversion;
      protected final DataConversion valueDataConversion;

      BaseMutation(DataConversion keyDataConversion, DataConversion valueDataConversion) {
         this.keyDataConversion = keyDataConversion;
         this.valueDataConversion = valueDataConversion;
      }

      public DataConversion keyDataConversion() {
         return keyDataConversion;
      }

      public DataConversion valueDataConversion() {
         return valueDataConversion;
      }

      @Override
      public void inject(ComponentRegistry registry) {
         GlobalConfiguration globalConfiguration = registry.getGlobalComponentRegistry().getGlobalConfiguration();
         EncoderRegistry encoderRegistry = registry.getComponent(EncoderRegistry.class);
         Configuration configuration = registry.getComponent(Configuration.class);
         keyDataConversion.injectDependencies(globalConfiguration, encoderRegistry, configuration);
         valueDataConversion.injectDependencies(globalConfiguration, encoderRegistry, configuration);
      }
   }

   static class ReadWrite<K, V, R> extends BaseMutation<K, V, R> {
      static final byte TYPE = 0;
      private final Function<EntryView.ReadWriteEntryView<K, V>, R> f;

      public ReadWrite(DataConversion keyDataConversion, DataConversion valueDataConversion, Function<EntryView.ReadWriteEntryView<K, V>, R> f) {
         super(keyDataConversion, valueDataConversion);
         this.f = f;
      }

      @Override
      public byte type() {
         return TYPE;
      }

      @Override
      public R apply(EntryView.ReadWriteEntryView<K, V> view) {
         return f.apply(view);
      }
   }

   static class ReadWriteWithValue<K, V, R> extends BaseMutation<K, V, R> {
      static final byte TYPE = 1;
      private final Object value;
      private final BiFunction<V, EntryView.ReadWriteEntryView<K, V>, R> f;

      public ReadWriteWithValue(DataConversion keyDataConversion, DataConversion valueDataConversion, Object value, BiFunction<V, EntryView.ReadWriteEntryView<K, V>, R> f) {
         super(keyDataConversion, valueDataConversion);
         this.value = value;
         this.f = f;
      }

      @Override
      public byte type() {
         return TYPE;
      }

      @Override
      public R apply(EntryView.ReadWriteEntryView<K, V> view) {
         return f.apply((V) valueDataConversion.fromStorage(value), view);
      }
   }

   static class Write<K, V> extends BaseMutation<K, V, Void> {
      static final byte TYPE = 2;
      private final Consumer<EntryView.WriteEntryView<V>> f;

      public Write(DataConversion keyDataConversion, DataConversion valueDataConversion, Consumer<EntryView.WriteEntryView<V>> f) {
         super(keyDataConversion, valueDataConversion);
         this.f = f;
      }

      @Override
      public byte type() {
         return TYPE;
      }

      @Override
      public Void apply(EntryView.ReadWriteEntryView<K, V> view) {
         f.accept(view);
         return null;
      }
   }

   static class WriteWithValue<K, V> extends BaseMutation<K, V, Void> {
      static final byte TYPE = 3;
      private final Object value;
      private final BiConsumer<V, EntryView.WriteEntryView<V>> f;

      public WriteWithValue(DataConversion keyDataConversion, DataConversion valueDataConversion, Object value, BiConsumer<V, EntryView.WriteEntryView<V>> f) {
         super(keyDataConversion, valueDataConversion);
         this.value = value;
         this.f = f;
      }

      @Override
      public byte type() {
         return TYPE;
      }

      @Override
      public Void apply(EntryView.ReadWriteEntryView<K, V> view) {
         f.accept((V) valueDataConversion.fromStorage(value), view);
         return null;
      }
   }
}
