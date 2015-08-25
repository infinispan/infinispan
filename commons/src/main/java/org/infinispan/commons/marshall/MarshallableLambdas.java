package org.infinispan.commons.marshall;

import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public final class MarshallableLambdas {

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> setValueReturnPrevOrNull() {
      return SetValueReturnPrevOrNull.getInstance();
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> setValueReturnView() {
      return SetValueReturnView.getInstance();
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> setValueIfAbsentReturnPrevOrNull() {
      return SetValueIfAbsentReturnPrevOrNull.getInstance();
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> setValueIfAbsentReturnBoolean() {
      return SetValueIfAbsentReturnBoolean.getInstance();
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> setValueIfPresentReturnPrevOrNull() {
      return SetValueIfPresentReturnPrevOrNull.getInstance();
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> setValueIfPresentReturnBoolean() {
      return SetValueIfPresentReturnBoolean.getInstance();
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> setValueIfEqualsReturnBoolean(V oldValue) {
      return new SetValueIfEqualsReturnBoolean<>(oldValue);
   }

   public static <K, V> Function<ReadWriteEntryView<K, V>, V> removeReturnPrevOrNull() {
      return RemoveReturnPrevOrNull.getInstance();
   }

   public static <K, V> Function<ReadWriteEntryView<K, V>, Boolean> removeReturnBoolean() {
      return RemoveReturnBoolean.getInstance();
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> removeIfValueEqualsReturnBoolean() {
      return RemoveIfValueEqualsReturnBoolean.getInstance();
   }

   public static <V> BiConsumer<V, WriteEntryView<V>> setValueConsumer() {
      return SetValue.getInstance();
   }

   public static <V> Consumer<WriteEntryView<V>> removeConsumer() {
      return Remove.getInstance();
   }

   public static <K, V> Function<ReadWriteEntryView<K, V>, Optional<V>> returnReadWriteFind() {
      return ReturnReadWriteFind.getInstance();
   }

   public static <K, V> Function<ReadWriteEntryView<K, V>, V> returnReadWriteGet() {
      return ReturnReadWriteGet.getInstance();
   }

   public static <K, V> Function<ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> returnReadWriteView() {
      return ReturnReadWriteView.getInstance();
   }

   private static final class SetValueReturnPrevOrNull<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, V> {
      @Override
      public V apply(V v, ReadWriteEntryView<K, V> rw) {
         V prev = rw.find().orElse(null);
         rw.set(v);
         return prev;
      }

      private static final SetValueReturnPrevOrNull INSTANCE = new SetValueReturnPrevOrNull<>();
      @SuppressWarnings("unchecked")
      private static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> getInstance() {
         return SetValueReturnPrevOrNull.INSTANCE;
      }
   }

   private static final class SetValueReturnView<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> {
      @Override
      public ReadWriteEntryView<K, V> apply(V v, ReadWriteEntryView<K, V> rw) {
         rw.set(v);
         return rw;
      }

      private static final SetValueReturnView INSTANCE = new SetValueReturnView<>();
      @SuppressWarnings("unchecked")
      private static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> getInstance() {
         return SetValueReturnView.INSTANCE;
      }
   }

   private static final class SetValueIfAbsentReturnPrevOrNull<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, V> {
      @Override
      public V apply(V v, ReadWriteEntryView<K, V> rw) {
         Optional<V> opt = rw.find();
         V prev = opt.orElse(null);
         if (!opt.isPresent())
            rw.set(v);

         return prev;
      }

      private static final SetValueIfAbsentReturnPrevOrNull INSTANCE =
         new SetValueIfAbsentReturnPrevOrNull<>();
      @SuppressWarnings("unchecked")
      private static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> getInstance() {
         return SetValueIfAbsentReturnPrevOrNull.INSTANCE;
      }
   }

   private static final class SetValueIfAbsentReturnBoolean<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, Boolean> {
      @Override
      public Boolean apply(V v, ReadWriteEntryView<K, V> rw) {
         Optional<V> opt = rw.find();
         boolean success = !opt.isPresent();
         if (success) rw.set(v);
         return success;
      }

      private static final SetValueIfAbsentReturnBoolean INSTANCE =
         new SetValueIfAbsentReturnBoolean<>();
      @SuppressWarnings("unchecked")
      private static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> getInstance() {
         return SetValueIfAbsentReturnBoolean.INSTANCE;
      }
   }

   private static final class SetValueIfPresentReturnPrevOrNull<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, V> {
      @Override
      public V apply(V v, ReadWriteEntryView<K, V> rw) {
         return rw.find().map(prev -> {
            rw.set(v);
            return prev;
         }).orElse(null);
      }

      private static final SetValueIfPresentReturnPrevOrNull INSTANCE =
         new SetValueIfPresentReturnPrevOrNull<>();
      @SuppressWarnings("unchecked")
      private static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> getInstance() {
         return SetValueIfPresentReturnPrevOrNull.INSTANCE;
      }
   }

   private static final class SetValueIfPresentReturnBoolean<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, Boolean> {
      @Override
      public Boolean apply(V v, ReadWriteEntryView<K, V> rw) {
         return rw.find().map(prev -> {
            rw.set(v);
            return true;
         }).orElse(false);
      }

      private static final SetValueIfPresentReturnBoolean INSTANCE =
         new SetValueIfPresentReturnBoolean<>();
      @SuppressWarnings("unchecked")
      private static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> getInstance() {
         return SetValueIfPresentReturnBoolean.INSTANCE;
      }
   }

   static final class SetValueIfEqualsReturnBoolean<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, Boolean> {
      final V oldValue;

      public SetValueIfEqualsReturnBoolean(V oldValue) {
         this.oldValue = oldValue;
      }

      @Override
      public Boolean apply(V v, ReadWriteEntryView<K, V> rw) {
         return rw.find().map(prev -> {
            if (prev.equals(oldValue)) {
               rw.set(v);
               return true;
            }
            return false;
         }).orElse(false);
      }
   }

   private static final class RemoveReturnPrevOrNull<K, V>
         implements Function<ReadWriteEntryView<K, V>, V> {
      @Override
      public V apply(ReadWriteEntryView<K, V> rw) {
         V prev = rw.find().orElse(null);
         rw.remove();
         return prev;
      }

      private static final RemoveReturnPrevOrNull INSTANCE = new RemoveReturnPrevOrNull<>();
      @SuppressWarnings("unchecked")
      private static <K, V> Function<ReadWriteEntryView<K, V>, V> getInstance() {
         return RemoveReturnPrevOrNull.INSTANCE;
      }
   }

   private static final class RemoveReturnBoolean<K, V>
         implements Function<ReadWriteEntryView<K, V>, Boolean> {
      @Override
      public Boolean apply(ReadWriteEntryView<K, V> rw) {
         boolean success = rw.find().isPresent();
         rw.remove();
         return success;
      }

      private static final RemoveReturnBoolean INSTANCE = new RemoveReturnBoolean<>();
      @SuppressWarnings("unchecked")
      private static <K, V> Function<ReadWriteEntryView<K, V>, Boolean> getInstance() {
         return RemoveReturnBoolean.INSTANCE;
      }
   }

   private static final class RemoveIfValueEqualsReturnBoolean<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, Boolean> {
      @Override
      public Boolean apply(V v, ReadWriteEntryView<K, V> rw) {
         return rw.find().map(prev -> {
            if (prev.equals(v)) {
               rw.remove();
               return true;
            }

            return false;
         }).orElse(false);
      }

      private static final RemoveIfValueEqualsReturnBoolean INSTANCE =
         new RemoveIfValueEqualsReturnBoolean<>();
      @SuppressWarnings("unchecked")
      private static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> getInstance() {
         return RemoveIfValueEqualsReturnBoolean.INSTANCE;
      }
   }

   private static final class SetValue<V> implements BiConsumer<V, WriteEntryView<V>> {
      @Override
      public void accept(V v, WriteEntryView<V> wo) {
         wo.set(v);
      }

      private static final SetValue INSTANCE = new SetValue<>();
      @SuppressWarnings("unchecked")
      private static <K, V> BiConsumer<V, WriteEntryView<V>> getInstance() {
         return SetValue.INSTANCE;
      }
   }

   private static final class Remove<V> implements Consumer<WriteEntryView<V>> {
      @Override
      public void accept(WriteEntryView<V> wo) {
         wo.remove();
      }

      private static final Remove INSTANCE = new Remove<>();
      @SuppressWarnings("unchecked")
      private static <V> Consumer<WriteEntryView<V>> getInstance() {
         return Remove.INSTANCE;
      }
   }

   private static final class ReturnReadWriteFind<K, V>
         implements Function<ReadWriteEntryView<K, V>, Optional<V>> {
      @Override
      public Optional<V> apply(ReadWriteEntryView<K, V> rw) {
         return rw.find();
      }

      private static final ReturnReadWriteFind INSTANCE = new ReturnReadWriteFind<>();
      @SuppressWarnings("unchecked")
      private static <K, V> Function<ReadWriteEntryView<K, V>, Optional<V>> getInstance() {
         return ReturnReadWriteFind.INSTANCE;
      }
   }

   private static final class ReturnReadWriteGet<K, V>
      implements Function<ReadWriteEntryView<K, V>, V> {
      @Override
      public V apply(ReadWriteEntryView<K, V> rw) {
         return rw.get();
      }

      private static final ReturnReadWriteGet INSTANCE = new ReturnReadWriteGet<>();
      @SuppressWarnings("unchecked")
      private static <K, V> Function<ReadWriteEntryView<K, V>, V> getInstance() {
         return ReturnReadWriteGet.INSTANCE;
      }
   }

   private static final class ReturnReadWriteView<K, V>
      implements Function<ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> {
      @Override
      public ReadWriteEntryView<K, V> apply(ReadWriteEntryView<K, V> rw) {
         return rw;
      }

      private static final ReturnReadWriteView INSTANCE = new ReturnReadWriteView<>();
      @SuppressWarnings("unchecked")
      private static <K, V> Function<ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> getInstance() {
         return ReturnReadWriteView.INSTANCE;
      }
   }

   private MarshallableLambdas() {
      // No-op, holds static variables
   }

}
