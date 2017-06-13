package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.functional.EntryView;

/**
 * Helper class for marshalling, also hiding implementations of {@link Mutation} from the interface.
 */
final class Mutations {
   private Mutations() {}

   // No need to occupy externalizer ids when we have a limited set of options
   static <K, V, R> void writeTo(ObjectOutput output, Mutation<K, V, R> mutation) throws IOException {
      byte type = mutation.type();
      output.writeByte(type);
      switch (type) {
         case ReadWrite.TYPE:
            output.writeObject(((ReadWrite<K, V, ?>) mutation).f);
            break;
         case ReadWriteWithValue.TYPE:
            output.writeObject(((ReadWriteWithValue<K, V, R>) mutation).value);
            output.writeObject(((ReadWriteWithValue<K, V, R>) mutation).f);
            break;
         case Write.TYPE:
            output.writeObject(((Write<K, V>) mutation).f);
            break;
         case WriteWithValue.TYPE:
            output.writeObject(((WriteWithValue<K, V>) mutation).value);
            output.writeObject(((WriteWithValue<K, V>) mutation).f);
            break;
      }
   }

   static <K, V> Mutation<K, V, ?> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      switch (input.readByte()) {
         case ReadWrite.TYPE:
            return new ReadWrite<>((Function<EntryView.ReadWriteEntryView<K, V>, ?>) input.readObject());
         case ReadWriteWithValue.TYPE:
            return new ReadWriteWithValue<>((V) input.readObject(), (BiFunction<V, EntryView.ReadWriteEntryView<K, V>, ?>) input.readObject());
         case Write.TYPE:
            return new Write<K, V>((Consumer<EntryView.WriteEntryView<V>>) input.readObject());
         case WriteWithValue.TYPE:
            return new WriteWithValue<K, V>((V) input.readObject(), (BiConsumer<V, EntryView.WriteEntryView<V>>) input.readObject());
         default:
            throw new IllegalStateException("Unknown type of mutation, broken input?");
      }
   }

   static class ReadWrite<K, V, R> implements Mutation<K, V, R> {
      static final byte TYPE = 0;
      private final Function<EntryView.ReadWriteEntryView<K, V>, R> f;

      public ReadWrite(Function<EntryView.ReadWriteEntryView<K, V>, R> f) {
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

   static class ReadWriteWithValue<K, V, R> implements Mutation<K, V, R> {
      static final byte TYPE = 1;
      private final V value;
      private final BiFunction<V, EntryView.ReadWriteEntryView<K, V>, R> f;

      public ReadWriteWithValue(V value, BiFunction<V, EntryView.ReadWriteEntryView<K, V>, R> f) {
         this.value = value;
         this.f = f;
      }

      @Override
      public byte type() {
         return TYPE;
      }

      @Override
      public R apply(EntryView.ReadWriteEntryView<K, V> view) {
         return f.apply(value, view);
      }
   }

   static class Write<K, V> implements Mutation<K, V, Void> {
      static final byte TYPE = 2;
      private final Consumer<EntryView.WriteEntryView<V>> f;

      public Write(Consumer<EntryView.WriteEntryView<V>> f) {
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

   static class WriteWithValue<K, V> implements Mutation<K, V, Void> {
      static final byte TYPE = 3;
      private final V value;
      private final BiConsumer<V, EntryView.WriteEntryView<V>> f;

      public WriteWithValue(V value, BiConsumer<V, EntryView.WriteEntryView<V>> f) {
         this.value = value;
         this.f = f;
      }

      @Override
      public byte type() {
         return TYPE;
      }

      @Override
      public Void apply(EntryView.ReadWriteEntryView<K, V> view) {
         f.accept(value, view);
         return null;
      }
   }
}
