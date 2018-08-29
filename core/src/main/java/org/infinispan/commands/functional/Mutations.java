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
   private Mutations() {
   }

   // No need to occupy externalizer ids when we have a limited set of options
   static <K, V, T, R> void writeTo(ObjectOutput output, Mutation<K, V, R> mutation) throws IOException {
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
            ReadWriteWithValue<K, V, T, R> rwwv = (ReadWriteWithValue<K, V, T, R>) mutation;
            output.writeObject(rwwv.argument);
            output.writeObject(rwwv.f);
            break;
         case Write.TYPE:
            output.writeObject(((Write<K, V>) mutation).f);
            break;
         case WriteWithValue.TYPE:
            WriteWithValue<K, V, T> wwv = (WriteWithValue<K, V, T>) mutation;
            output.writeObject(wwv.argument);
            output.writeObject(wwv.f);
            break;
      }
   }

   static <K, V, T> Mutation<K, V, ?> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      DataConversion keyDataConversion = DataConversion.readFrom(input);
      DataConversion valueDataConversion = DataConversion.readFrom(input);
      switch (input.readByte()) {
         case ReadWrite.TYPE:
            return new ReadWrite<>(keyDataConversion, valueDataConversion, (Function<EntryView.ReadWriteEntryView<K, V>, ?>) input.readObject());
         case ReadWriteWithValue.TYPE:
            return new ReadWriteWithValue<>(keyDataConversion, valueDataConversion, input.readObject(), (BiFunction<V, EntryView.ReadWriteEntryView<K, V>, ?>) input.readObject());
         case Write.TYPE:
            return new Write<>(keyDataConversion, valueDataConversion, (Consumer<EntryView.WriteEntryView<K, V>>) input.readObject());
         case WriteWithValue.TYPE:
            return new WriteWithValue<>(keyDataConversion, valueDataConversion, input.readObject(), (BiConsumer<T, EntryView.WriteEntryView<K, V>>) input.readObject());
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

   static class ReadWriteWithValue<K, V, T, R> extends BaseMutation<K, V, R> {
      static final byte TYPE = 1;
      private final Object argument;
      private final BiFunction<T, EntryView.ReadWriteEntryView<K, V>, R> f;

      public ReadWriteWithValue(DataConversion keyDataConversion, DataConversion valueDataConversion, Object argument, BiFunction<T, EntryView.ReadWriteEntryView<K, V>, R> f) {
         super(keyDataConversion, valueDataConversion);
         this.argument = argument;
         this.f = f;
      }

      @Override
      public byte type() {
         return TYPE;
      }

      @Override
      public R apply(EntryView.ReadWriteEntryView<K, V> view) {
         return f.apply((T) valueDataConversion.fromStorage(argument), view);
      }
   }

   static class Write<K, V> extends BaseMutation<K, V, Void> {
      static final byte TYPE = 2;
      private final Consumer<EntryView.WriteEntryView<K, V>> f;

      public Write(DataConversion keyDataConversion, DataConversion valueDataConversion, Consumer<EntryView.WriteEntryView<K, V>> f) {
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

   static class WriteWithValue<K, V, T> extends BaseMutation<K, V, Void> {
      static final byte TYPE = 3;
      private final Object argument;
      private final BiConsumer<T, EntryView.WriteEntryView<K, V>> f;

      public WriteWithValue(DataConversion keyDataConversion, DataConversion valueDataConversion, Object argument, BiConsumer<T, EntryView.WriteEntryView<K, V>> f) {
         super(keyDataConversion, valueDataConversion);
         this.argument = argument;
         this.f = f;
      }

      @Override
      public byte type() {
         return TYPE;
      }

      @Override
      public Void apply(EntryView.ReadWriteEntryView<K, V> view) {
         f.accept((T) valueDataConversion.fromStorage(argument), view);
         return null;
      }
   }
}
