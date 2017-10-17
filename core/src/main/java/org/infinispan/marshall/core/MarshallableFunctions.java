package org.infinispan.marshall.core;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.MetaParam;

public final class MarshallableFunctions {

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> setValueReturnPrevOrNull() {
      return SetValueReturnPrevOrNull.getInstance();
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> setValueMetasReturnPrevOrNull(MetaParam.Writable... metas) {
      return new SetValueMetasReturnPrevOrNull<>(metas);
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> setValueReturnView() {
      return SetValueReturnView.getInstance();
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> setValueMetasReturnView(MetaParam.Writable... metas) {
      return new SetValueMetasReturnView<>(metas);
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> setValueIfAbsentReturnPrevOrNull() {
      return SetValueIfAbsentReturnPrevOrNull.getInstance();
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> setValueMetasIfAbsentReturnPrevOrNull(MetaParam.Writable... metas) {
      return new SetValueMetasIfAbsentReturnPrevOrNull<>(metas);
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> setValueIfAbsentReturnBoolean() {
      return SetValueIfAbsentReturnBoolean.getInstance();
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> setValueMetasIfAbsentReturnBoolean(MetaParam.Writable... metas) {
      return new SetValueMetasIfAbsentReturnBoolean<>(metas);
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> setValueIfPresentReturnPrevOrNull() {
      return SetValueIfPresentReturnPrevOrNull.getInstance();
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> setValueMetasIfPresentReturnPrevOrNull(MetaParam.Writable... metas) {
      return new SetValueMetasIfPresentReturnPrevOrNull<>(metas);
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> setValueIfPresentReturnBoolean() {
      return SetValueIfPresentReturnBoolean.getInstance();
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> setValueMetasIfPresentReturnBoolean(MetaParam.Writable... metas) {
      return new SetValueMetasIfPresentReturnBoolean<>(metas);
   }

   public static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> setValueIfEqualsReturnBoolean(V oldValue, MetaParam.Writable... metas) {
      return new SetValueIfEqualsReturnBoolean<>(oldValue, metas);
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

   public static <K, V> BiConsumer<V, WriteEntryView<K, V>> setValueConsumer() {
      return SetValue.getInstance();
   }

   public static <K, V> BiConsumer<V, WriteEntryView<K, V>> setValueMetasConsumer(MetaParam.Writable... metas) {
      return new SetValueMetas<>(metas);
   }

   public static <K, V> BiConsumer<V, WriteEntryView<K, V>> setInternalCacheValueConsumer() {
      return SetInternalCacheValue.getInstance();
   }

   public static <K, V> Consumer<WriteEntryView<K, V>> removeConsumer() {
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

   public static <K, V> Function<ReadEntryView<K, V>, V> returnReadOnlyFindOrNull() {
      return ReturnReadOnlyFindOrNull.getInstance();
   }

   public static <K, V> Function<ReadEntryView<K, V>, Boolean> returnReadOnlyFindIsPresent() {
      return ReturnReadOnlyFindIsPresent.getInstance();
   }

   public static <T> Function<T, T> identity() {
      return Identity.getInstance();
   }

   private static abstract class AbstractSetValueReturnPrevOrNull<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, V> {
      final MetaParam.Writable[] metas;

      protected AbstractSetValueReturnPrevOrNull(MetaParam.Writable[] metas) {
         this.metas = metas;
      }

      @Override
      public V apply(V v, ReadWriteEntryView<K, V> rw) {
         V prev = rw.find().orElse(null);
         rw.set(v, metas);
         return prev;
      }
   }

   private static final class SetValueReturnPrevOrNull<K, V> extends AbstractSetValueReturnPrevOrNull<K, V> {
      protected SetValueReturnPrevOrNull(MetaParam.Writable[] metas) {
         super(metas);
      }

      private static final SetValueReturnPrevOrNull INSTANCE =
         new SetValueReturnPrevOrNull<>(new MetaParam.Writable[0]);
      @SuppressWarnings("unchecked")
      private static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> getInstance() {
         return SetValueReturnPrevOrNull.INSTANCE;
      }
   }

   interface LambdaWithMetas {
      MetaParam.Writable[] metas();
   }

   static final class SetValueMetasReturnPrevOrNull<K, V>
         extends AbstractSetValueReturnPrevOrNull<K, V> implements LambdaWithMetas {
      SetValueMetasReturnPrevOrNull(MetaParam.Writable[] metas) {
         super(metas);
      }

      @Override
      public MetaParam.Writable[] metas() {
         return metas;
      }
   }

   private static abstract class AbstractSetValueReturnView<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> {
      final MetaParam.Writable[] metas;

      protected AbstractSetValueReturnView(MetaParam.Writable[] metas) {
         this.metas = metas;
      }

      @Override
      public ReadWriteEntryView<K, V> apply(V v, ReadWriteEntryView<K, V> rw) {
         rw.set(v);
         return rw;
      }
   }

   private static final class SetValueReturnView<K, V> extends AbstractSetValueReturnView<K, V> {
      protected SetValueReturnView(MetaParam.Writable[] metas) {
         super(metas);
      }

      private static final SetValueReturnView INSTANCE = new SetValueReturnView<>(new MetaParam.Writable[]{});
      @SuppressWarnings("unchecked")
      private static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> getInstance() {
         return SetValueReturnView.INSTANCE;
      }
   }

   static final class SetValueMetasReturnView<K, V> extends AbstractSetValueReturnView<K, V> implements LambdaWithMetas {
      protected SetValueMetasReturnView(MetaParam.Writable[] metas) {
         super(metas);
      }

      @Override
      public MetaParam.Writable[] metas() {
         return metas;
      }
   }

   private static abstract class AbstractSetValueIfAbsentReturnPrevOrNull<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, V> {
      final MetaParam.Writable[] metas;

      protected AbstractSetValueIfAbsentReturnPrevOrNull(MetaParam.Writable[] metas) {
         this.metas = metas;
      }

      @Override
      public V apply(V v, ReadWriteEntryView<K, V> rw) {
         Optional<V> opt = rw.find();
         V prev = opt.orElse(null);
         if (!opt.isPresent())
            rw.set(v, metas);

         return prev;
      }
   }

   private static final class SetValueIfAbsentReturnPrevOrNull<K, V>
      extends AbstractSetValueIfAbsentReturnPrevOrNull<K, V> {
      protected SetValueIfAbsentReturnPrevOrNull(MetaParam.Writable[] metas) {
         super(metas);
      }

      private static final SetValueIfAbsentReturnPrevOrNull INSTANCE =
         new SetValueIfAbsentReturnPrevOrNull<>(new MetaParam.Writable[]{});
      @SuppressWarnings("unchecked")
      private static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> getInstance() {
         return SetValueIfAbsentReturnPrevOrNull.INSTANCE;
      }
   }

   static final class SetValueMetasIfAbsentReturnPrevOrNull<K, V>
         extends AbstractSetValueIfAbsentReturnPrevOrNull<K, V> implements LambdaWithMetas {
      protected SetValueMetasIfAbsentReturnPrevOrNull(MetaParam.Writable[] metas) {
         super(metas);
      }

      @Override
      public MetaParam.Writable[] metas() {
         return metas;
      }
   }

   private static abstract class AbstractSetValueIfAbsentReturnBoolean<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, Boolean> {
      final MetaParam.Writable[] metas;

      private AbstractSetValueIfAbsentReturnBoolean(MetaParam.Writable[] metas) {
         this.metas = metas;
      }

      @Override
      public Boolean apply(V v, ReadWriteEntryView<K, V> rw) {
         Optional<V> opt = rw.find();
         boolean success = !opt.isPresent();
         if (success) rw.set(v, metas);
         return success;
      }
   }

   private static final class SetValueIfAbsentReturnBoolean<K, V>
      extends AbstractSetValueIfAbsentReturnBoolean<K, V> {
      private SetValueIfAbsentReturnBoolean(MetaParam.Writable[] metas) {
         super(metas);
      }

      private static final SetValueIfAbsentReturnBoolean INSTANCE =
         new SetValueIfAbsentReturnBoolean<>(new MetaParam.Writable[]{});
      @SuppressWarnings("unchecked")
      private static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> getInstance() {
         return SetValueIfAbsentReturnBoolean.INSTANCE;
      }
   }

   static final class SetValueMetasIfAbsentReturnBoolean<K, V>
         extends AbstractSetValueIfAbsentReturnBoolean<K, V> implements LambdaWithMetas {
      SetValueMetasIfAbsentReturnBoolean(MetaParam.Writable[] metas) {
         super(metas);
      }

      @Override
      public MetaParam.Writable[] metas() {
         return metas;
      }
   }

   private static abstract class AbstractSetValueIfPresentReturnPrevOrNull<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, V> {
      final MetaParam.Writable[] metas;

      protected AbstractSetValueIfPresentReturnPrevOrNull(MetaParam.Writable[] metas) {
         this.metas = metas;
      }

      @Override
      public V apply(V v, ReadWriteEntryView<K, V> rw) {
         return rw.find().map(prev -> {
            rw.set(v, metas);
            return prev;
         }).orElse(null);
      }
   }

   private static final class SetValueIfPresentReturnPrevOrNull<K, V>
         extends AbstractSetValueIfPresentReturnPrevOrNull<K, V> {
      protected SetValueIfPresentReturnPrevOrNull(MetaParam.Writable[] metas) {
         super(metas);
      }

      private static final SetValueIfPresentReturnPrevOrNull INSTANCE =
         new SetValueIfPresentReturnPrevOrNull<>(new MetaParam.Writable[]{});
      @SuppressWarnings("unchecked")
      private static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, V> getInstance() {
         return SetValueIfPresentReturnPrevOrNull.INSTANCE;
      }
   }

   static final class SetValueMetasIfPresentReturnPrevOrNull<K, V>
         extends AbstractSetValueIfPresentReturnPrevOrNull<K, V> implements LambdaWithMetas {
      protected SetValueMetasIfPresentReturnPrevOrNull(MetaParam.Writable[] metas) {
         super(metas);
      }

      @Override
      public MetaParam.Writable[] metas() {
         return metas;
      }
   }

   private static abstract class AbstractSetValueIfPresentReturnBoolean<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, Boolean> {
      final MetaParam.Writable[] metas;

      private AbstractSetValueIfPresentReturnBoolean(MetaParam.Writable[] metas) {
         this.metas = metas;
      }

      @Override
      public Boolean apply(V v, ReadWriteEntryView<K, V> rw) {
         return rw.find().map(prev -> {
            rw.set(v, metas);
            return true;
         }).orElse(false);
      }
   }

   private static final class SetValueIfPresentReturnBoolean<K, V>
      extends AbstractSetValueIfPresentReturnBoolean<K, V> {
      private SetValueIfPresentReturnBoolean(MetaParam.Writable[] metas) {
         super(metas);
      }

      private static final SetValueIfPresentReturnBoolean INSTANCE =
         new SetValueIfPresentReturnBoolean<>(new MetaParam.Writable[]{});
      @SuppressWarnings("unchecked")
      private static <K, V> BiFunction<V, ReadWriteEntryView<K, V>, Boolean> getInstance() {
         return SetValueIfPresentReturnBoolean.INSTANCE;
      }
   }

   static final class SetValueMetasIfPresentReturnBoolean<K, V>
         extends AbstractSetValueIfPresentReturnBoolean<K, V> implements LambdaWithMetas {
      SetValueMetasIfPresentReturnBoolean(MetaParam.Writable[] metas) {
         super(metas);
      }

      @Override
      public MetaParam.Writable[] metas() {
         return metas;
      }
   }

   static final class SetValueIfEqualsReturnBoolean<K, V>
         implements BiFunction<V, ReadWriteEntryView<K, V>, Boolean>, LambdaWithMetas {
      final V oldValue;
      final MetaParam.Writable[] metas;

      public SetValueIfEqualsReturnBoolean(V oldValue, MetaParam.Writable[] metas) {
         this.oldValue = oldValue;
         this.metas = metas;
      }

      @Override
      public Boolean apply(V v, ReadWriteEntryView<K, V> rw) {
         return rw.find().map(prev -> {
            if (prev.equals(oldValue)) {
               rw.set(v, metas);
               return true;
            }
            return false;
         }).orElse(false);
      }

      @Override
      public MetaParam.Writable[] metas() {
         return metas;
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

   private static abstract class AbstractSetValue<K, V> implements BiConsumer<V, WriteEntryView<K, V>> {
      final MetaParam.Writable[] metas;

      protected AbstractSetValue(MetaParam.Writable[] metas) {
         this.metas = metas;
      }

      @Override
      public void accept(V v, WriteEntryView<K, V> wo) {
         wo.set(v, metas);
      }
   }

   private static final class SetValue<K, V> extends AbstractSetValue<K, V> {
      protected SetValue(MetaParam.Writable[] metas) {
         super(metas);
      }

      private static final SetValue INSTANCE = new SetValue<>(new MetaParam.Writable[0]);
      @SuppressWarnings("unchecked")
      private static <K, V> BiConsumer<V, WriteEntryView<K, V>> getInstance() {
         return SetValue.INSTANCE;
      }
   }

   static final class SetValueMetas<K, V> extends AbstractSetValue<K, V> implements LambdaWithMetas {
      SetValueMetas(MetaParam.Writable[] metas) {
         super(metas);
      }

      @Override
      public MetaParam.Writable[] metas() {
         return metas;
      }
   }

   private static final class SetInternalCacheValue<V> implements BiConsumer<InternalCacheValue<V>, WriteEntryView<?, V>> {
      private static final SetInternalCacheValue INSTANCE = new SetInternalCacheValue<>();
      @SuppressWarnings("unchecked")
      private static <K, V> BiConsumer<V, WriteEntryView<K, V>> getInstance() {
         return SetInternalCacheValue.INSTANCE;
      }

      @Override
      public void accept(InternalCacheValue<V> icv, WriteEntryView<?, V> view) {
         view.set(icv.getValue(), icv.getMetadata());
      }
   }

   private static final class Remove<V> implements Consumer<WriteEntryView<?, V>> {
      @Override
      public void accept(WriteEntryView<?, V> wo) {
         wo.remove();
      }

      private static final Remove INSTANCE = new Remove<>();
      @SuppressWarnings("unchecked")
      private static <K, V> Consumer<WriteEntryView<K, V>> getInstance() {
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

   private static final class ReturnReadOnlyFindOrNull<K, V>
      implements Function<ReadEntryView<K, V>, V> {
      @Override
      public V apply(ReadEntryView<K, V> ro) {
         return ro.find().orElse(null);
      }

      private static final ReturnReadOnlyFindOrNull INSTANCE = new ReturnReadOnlyFindOrNull<>();

      private static <K, V> Function<ReadEntryView<K, V>, V> getInstance() {
         return INSTANCE;
      }
   }

   private static final class ReturnReadOnlyFindIsPresent<K, V>
      implements Function<ReadEntryView<K, V>, Boolean> {
      @Override
      public Boolean apply(ReadEntryView<K, V> ro) {
         return ro.find().isPresent();
      }

      private static final ReturnReadOnlyFindIsPresent INSTANCE = new ReturnReadOnlyFindIsPresent<>();

      private static <K, V> Function<ReadEntryView<K, V>, Boolean> getInstance() {
         return INSTANCE;
      }
   }

   private static final class Identity<T> implements Function<T, T> {
      @Override
      public T apply(T o) {
         return o;
      }

      private static final Identity INSTANCE = new Identity<>();

      private static <T> Function<T, T> getInstance() {
         return INSTANCE;
      }
   }

   private MarshallableFunctions() {
      // No-op, holds static variables
   }

}
