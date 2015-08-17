package org.infinispan.util.functional;

import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeValueMatcher;
import org.infinispan.commons.marshall.SerializeWith;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public final class MarshallableFunctionalInterfaces {

   @SuppressWarnings("unchecked")
   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> setValueReturnPrevOrNull() {
      return SetValueReturnPrevOrNull.INSTANCE;
   }

   @SuppressWarnings("unchecked")
   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> setValueReturnView() {
      return SetValueReturnView.INSTANCE;
   }

   @SuppressWarnings("unchecked")
   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> setValueIfAbsentReturnPrevOrNull() {
      return SetValueIfAbsentReturnPrevOrNull.INSTANCE;
   }

   @SuppressWarnings("unchecked")
   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> setValueIfAbsentReturnBoolean() {
      return SetValueIfAbsentReturnBoolean.INSTANCE;
   }

   @SuppressWarnings("unchecked")
   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> setValueIfPresentReturnPrevOrNull() {
      return SetValueIfPresentReturnPrevOrNull.INSTANCE;
   }

   @SuppressWarnings("unchecked")
   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> setValueIfPresentReturnBoolean() {
      return SetValueIfPresentReturnBoolean.INSTANCE;
   }

   @SuppressWarnings("unchecked")
   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> setValueIfEqualsReturnBoolean(V oldValue) {
      return new SetValueIfEqualsReturnBoolean<>(oldValue);
   }

   @SuppressWarnings("unchecked")
   public static <K, V> Function<ReadWriteEntryView<K, V>, V> removeReturnPrevOrNull() {
      return RemoveReturnPrevOrNull.INSTANCE;
   }

   @SuppressWarnings("unchecked")
   public static <K, V> Function<ReadWriteEntryView<K, V>, Boolean> removeReturnBoolean() {
      return RemoveReturnBoolean.INSTANCE;
   }

   @SuppressWarnings("unchecked")
   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> removeIfValueEqualsReturnBoolean() {
      return RemoveIfValueEqualsReturnBoolean.INSTANCE;
   }

   @SuppressWarnings("unchecked")
   public static <V> BiConsumer<V, WriteEntryView<V>> setValueConsumer() {
      return SetValue.INSTANCE;
   }

   @SuppressWarnings("unchecked")
   public static <V> Consumer<WriteEntryView<V>> removeConsumer() {
      return Remove.INSTANCE;
   }

   @SuppressWarnings("unchecked")
   public static <K, V> Function<ReadWriteEntryView<K, V>, Optional<V>> returnReadWriteFind() {
      return ReturnReadWriteFind.INSTANCE;
   }

   @SuppressWarnings("unchecked")
   public static <K, V> Function<ReadWriteEntryView<K, V>, V> returnReadWriteGet() {
      return ReturnReadWriteGet.INSTANCE;
   }

   @SuppressWarnings("unchecked")
   public static <K, V> Function<ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> returnReadWriteView() {
      return ReturnReadWriteView.INSTANCE;
   }

   @SerializeWith(value = SetValueReturnPrevOrNull.Externalizer0.class,
      valueMatcher = SerializeValueMatcher.MATCH_ALWAYS)
   private static final class SetValueReturnPrevOrNull<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, V> {
      @Override
      public V apply(V v, ReadWriteEntryView<K, V> rw) {
         V prev = rw.find().orElse(null);
         rw.set(v);
         return prev;
      }

      private static final SetValueReturnPrevOrNull INSTANCE = new SetValueReturnPrevOrNull<>();
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   @SerializeWith(value = SetValueReturnView.Externalizer0.class,
      valueMatcher = SerializeValueMatcher.MATCH_ALWAYS)
   private static final class SetValueReturnView<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> {
      @Override
      public ReadWriteEntryView<K, V> apply(V v, ReadWriteEntryView<K, V> rw) {
         rw.set(v);
         return rw;
      }

      private static final SetValueReturnView INSTANCE = new SetValueReturnView<>();
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   @SerializeWith(value = SetValueIfAbsentReturnPrevOrNull.Externalizer0.class,
      valueMatcher = SerializeValueMatcher.MATCH_EXPECTED)
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
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   @SerializeWith(value = SetValueIfAbsentReturnBoolean.Externalizer0.class,
      valueMatcher = SerializeValueMatcher.MATCH_EXPECTED)
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
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   @SerializeWith(value = SetValueIfPresentReturnPrevOrNull.Externalizer0.class,
      valueMatcher = SerializeValueMatcher.MATCH_NON_NULL)
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
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   @SerializeWith(value = SetValueIfPresentReturnBoolean.Externalizer0.class,
      valueMatcher = SerializeValueMatcher.MATCH_NON_NULL)
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
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   @SerializeWith(value = SetValueIfEqualsReturnBoolean.Externalizer0.class,
      valueMatcher = SerializeValueMatcher.MATCH_EXPECTED)
   private static final class SetValueIfEqualsReturnBoolean<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, Boolean> {
      private final V oldValue;

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

      public static final class Externalizer0 implements Externalizer<SetValueIfEqualsReturnBoolean<?, ?>> {
         public void writeObject(ObjectOutput oo, SetValueIfEqualsReturnBoolean<?, ?> o)
               throws IOException {
            oo.writeObject(o.oldValue);
         }
         public SetValueIfEqualsReturnBoolean<?, ?> readObject(ObjectInput input)
               throws IOException, ClassNotFoundException {
            Object oldValue = input.readObject();
            return new SetValueIfEqualsReturnBoolean<>(oldValue);
         }
      }
   }

   @SerializeWith(value = RemoveReturnPrevOrNull.Externalizer0.class,
      valueMatcher = SerializeValueMatcher.MATCH_ALWAYS)
   private static final class RemoveReturnPrevOrNull<K, V>
         implements Function<ReadWriteEntryView<K, V>, V> {
      @Override
      public V apply(ReadWriteEntryView<K, V> rw) {
         V prev = rw.find().orElse(null);
         rw.remove();
         return prev;
      }

      private static final RemoveReturnPrevOrNull INSTANCE = new RemoveReturnPrevOrNull<>();
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   @SerializeWith(value = RemoveReturnBoolean.Externalizer0.class,
      valueMatcher = SerializeValueMatcher.MATCH_ALWAYS)
   private static final class RemoveReturnBoolean<K, V>
         implements Function<ReadWriteEntryView<K, V>, Boolean> {
      @Override
      public Boolean apply(ReadWriteEntryView<K, V> rw) {
         boolean success = rw.find().isPresent();
         rw.remove();
         return success;
      }

      private static final RemoveReturnBoolean INSTANCE = new RemoveReturnBoolean<>();
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   @SerializeWith(value = RemoveIfValueEqualsReturnBoolean.Externalizer0.class,
      valueMatcher = SerializeValueMatcher.MATCH_EXPECTED)
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
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   @SerializeWith(value = SetValue.Externalizer0.class)
   private static final class SetValue<V> implements BiConsumer<V, WriteEntryView<V>> {
      @Override
      public void accept(V v, WriteEntryView<V> wo) {
         wo.set(v);
      }

      private static final SetValue INSTANCE = new SetValue<>();
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   @SerializeWith(value = Remove.Externalizer0.class)
   private static final class Remove<V> implements Consumer<WriteEntryView<V>> {
      @Override
      public void accept(WriteEntryView<V> wo) {
         wo.remove();
      }

      private static final Remove INSTANCE = new Remove<>();
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   @SerializeWith(value = ReturnReadWriteFind.Externalizer0.class)
   private static final class ReturnReadWriteFind<K, V>
         implements Function<ReadWriteEntryView<K, V>, Optional<V>> {
      @Override
      public Optional<V> apply(ReadWriteEntryView<K, V> rw) {
         return rw.find();
      }

      private static final ReturnReadWriteFind INSTANCE = new ReturnReadWriteFind<>();
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   @SerializeWith(value = ReturnReadWriteGet.Externalizer0.class)
   private static final class ReturnReadWriteGet<K, V>
      implements Function<ReadWriteEntryView<K, V>, V> {
      @Override
      public V apply(ReadWriteEntryView<K, V> rw) {
         return rw.get();
      }

      private static final ReturnReadWriteGet INSTANCE = new ReturnReadWriteGet<>();
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   @SerializeWith(value = ReturnReadWriteView.Externalizer0.class)
   private static final class ReturnReadWriteView<K, V>
      implements Function<ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> {
      @Override
      public ReadWriteEntryView<K, V> apply(ReadWriteEntryView<K, V> rw) {
         return rw;
      }

      private static final ReturnReadWriteView INSTANCE = new ReturnReadWriteView<>();
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   private  MarshallableFunctionalInterfaces() {
      // No-op, holds static variables
   }

}
