package org.infinispan.commands.functional;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Helper class for marshalling, also hiding implementations of {@link Mutation} from the interface.
 */
public final class Mutations {
   private Mutations() {
   }

   static abstract class BaseMutation<K, V, R> implements Mutation<K, V, R> {

      @ProtoField(number = 1)
      protected final DataConversion keyDataConversion;

      @ProtoField(number = 2)
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
         registry.wireDependencies(keyDataConversion);
         registry.wireDependencies(valueDataConversion);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MUTATIONS_READ_WRITE)
   public static class ReadWrite<K, V, R> extends BaseMutation<K, V, R> {

      private final Function<EntryView.ReadWriteEntryView<K, V>, R> f;

      ReadWrite(DataConversion keyDataConversion, DataConversion valueDataConversion, Function<EntryView.ReadWriteEntryView<K, V>, R> f) {
         super(keyDataConversion, valueDataConversion);
         this.f = f;
      }

      @ProtoFactory
      ReadWrite(DataConversion keyDataConversion, DataConversion valueDataConversion,
                MarshallableObject<Function<EntryView.ReadWriteEntryView<K, V>, R>> function) {
         this(keyDataConversion, valueDataConversion, MarshallableObject.unwrap(function));
      }

      @ProtoField(number = 3)
      MarshallableObject<Function<EntryView.ReadWriteEntryView<K, V>, R>> getFunction() {
         return MarshallableObject.create(f);
      }

      @Override
      public void inject(ComponentRegistry registry) {
         super.inject(registry);

         if(f instanceof InjectableComponent)         {
            ((InjectableComponent) f).inject(registry);
         }
      }

      @Override
      public R apply(EntryView.ReadWriteEntryView<K, V> view) {
         return f.apply(view);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MUTATIONS_READ_WRITE_WITH_VALUE)
   public static class ReadWriteWithValue<K, V, T, R> extends BaseMutation<K, V, R> {

      private final Object argument;
      private final BiFunction<T, EntryView.ReadWriteEntryView<K, V>, R> f;

      ReadWriteWithValue(DataConversion keyDataConversion, DataConversion valueDataConversion, Object argument, BiFunction<T, EntryView.ReadWriteEntryView<K, V>, R> f) {
         super(keyDataConversion, valueDataConversion);
         this.argument = argument;
         this.f = f;
      }

      @ProtoFactory
      ReadWriteWithValue(DataConversion keyDataConversion, DataConversion valueDataConversion,
                         MarshallableObject<?> argument,
                         MarshallableObject<BiFunction<T, EntryView.ReadWriteEntryView<K, V>, R>> biFunction) {
         this(keyDataConversion, valueDataConversion, MarshallableObject.unwrap(argument), MarshallableObject.unwrap(biFunction));
      }

      @ProtoField(number = 3)
      MarshallableObject<?> getArgument() {
         return MarshallableObject.create(argument);
      }

      @ProtoField(number = 4, name = "function")
      MarshallableObject<BiFunction<T, EntryView.ReadWriteEntryView<K, V>, R>> getBiFunction() {
         return MarshallableObject.create(f);
      }

      @Override
      public R apply(EntryView.ReadWriteEntryView<K, V> view) {
         return f.apply((T) valueDataConversion.fromStorage(argument), view);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MUTATIONS_WRITE)
   public static class Write<K, V> extends BaseMutation<K, V, Void> {

      private final Consumer<EntryView.WriteEntryView<K, V>> f;

      Write(DataConversion keyDataConversion, DataConversion valueDataConversion, Consumer<EntryView.WriteEntryView<K, V>> f) {
         super(keyDataConversion, valueDataConversion);
         this.f = f;
      }

      @ProtoFactory
      Write(DataConversion keyDataConversion, DataConversion valueDataConversion,
            MarshallableObject<Consumer<EntryView.WriteEntryView<K, V>>> consumer) {
         this(keyDataConversion, valueDataConversion, MarshallableObject.unwrap(consumer));
      }

      @ProtoField(number = 3, name = "function")
      MarshallableObject<Consumer<EntryView.WriteEntryView<K, V>>> getConsumer() {
         return MarshallableObject.create(f);
      }

      @Override
      public Void apply(EntryView.ReadWriteEntryView<K, V> view) {
         f.accept(view);
         return null;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MUTATIONS_WRITE_WITH_VALUE)
   public static class WriteWithValue<K, V, T> extends BaseMutation<K, V, Void> {
      private final Object argument;
      private final BiConsumer<T, EntryView.WriteEntryView<K, V>> f;

      WriteWithValue(DataConversion keyDataConversion, DataConversion valueDataConversion, Object argument, BiConsumer<T, EntryView.WriteEntryView<K, V>> f) {
         super(keyDataConversion, valueDataConversion);
         this.argument = argument;
         this.f = f;
      }

      @ProtoFactory
      WriteWithValue(DataConversion keyDataConversion, DataConversion valueDataConversion,
                     MarshallableObject<?> argument, MarshallableObject<BiConsumer<T, EntryView.WriteEntryView<K, V>>> biConsumer) {
         this(keyDataConversion, valueDataConversion, MarshallableObject.unwrap(argument), MarshallableObject.unwrap(biConsumer));
      }

      @ProtoField(number = 3)
      MarshallableObject<?> getArgument() {
         return MarshallableObject.create(argument);
      }

      @ProtoField(number = 4)
      MarshallableObject<BiConsumer<T, EntryView.WriteEntryView<K, V>>> getBiConsumer() {
         return MarshallableObject.create(f);
      }

      @Override
      public Void apply(EntryView.ReadWriteEntryView<K, V> view) {
         f.accept((T) valueDataConversion.fromStorage(argument), view);
         return null;
      }
   }
}
