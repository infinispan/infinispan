package org.infinispan.marshall.core;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.InjectiveFunction;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.MetaParam;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

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

   public static <K, V> BiConsumer<InternalCacheValue<V>, WriteEntryView<K, V>> setInternalCacheValueConsumer() {
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

   private abstract static class AbstractParamWritableFunction {
      final MetaParam.Writable[] metas;

      AbstractParamWritableFunction(MetaParam.Writable[] metas) {
         this.metas = metas;
      }

      @ProtoFactory
      AbstractParamWritableFunction(MarshallableArray<MetaParam.Writable> metas) {
         this(MarshallableArray.unwrap(metas, new MetaParam.Writable[0]));
      }

      @ProtoField(1)
      MarshallableArray<MetaParam.Writable> getMetas() {
         return MarshallableArray.create(metas);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_SET_VALUE_RETURN_PREV_OR_NULL)
   public static final class SetValueReturnPrevOrNull<K, V> implements BiFunction<V, ReadWriteEntryView<K, V>, V> {

      private static final SetValueReturnPrevOrNull INSTANCE = new SetValueReturnPrevOrNull<>();

      @SuppressWarnings("unchecked")
      @ProtoFactory
      static <K, V> SetValueReturnPrevOrNull<K, V> getInstance() {
         return SetValueReturnPrevOrNull.INSTANCE;
      }

      @Override
      public V apply(V v, ReadWriteEntryView<K, V> rw) {
         V prev = rw.find().orElse(null);
         rw.set(v);
         return prev;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_SET_VALUE_METAS_RETURN_PREV_OR_NULL)
   public static final class SetValueMetasReturnPrevOrNull<K, V> extends AbstractParamWritableFunction
         implements BiFunction<V, ReadWriteEntryView<K, V>, V> {

      SetValueMetasReturnPrevOrNull(MetaParam.Writable[] metas) {
         super(metas);
      }

      @ProtoFactory
      SetValueMetasReturnPrevOrNull(MarshallableArray<MetaParam.Writable> metas) {
         super(metas);
      }

      @Override
      public V apply(V v, ReadWriteEntryView<K, V> rw) {
         V prev = rw.find().orElse(null);
         rw.set(v, metas);
         return prev;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_SET_VALUE_RETURN_VIEW)
   public static final class SetValueReturnView<K, V> implements BiFunction<V, ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> {

      private static final SetValueReturnView INSTANCE = new SetValueReturnView<>();

      @ProtoFactory
      @SuppressWarnings("unchecked")
      static <K, V> SetValueReturnView<K, V> getInstance() {
         return SetValueReturnView.INSTANCE;
      }

      @Override
      public ReadWriteEntryView<K, V> apply(V v, ReadWriteEntryView<K, V> rw) {
         rw.set(v);
         return rw;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_SET_VALUE_METAS_RETURN_VIEW)
   public static final class SetValueMetasReturnView<K, V> extends AbstractParamWritableFunction
         implements BiFunction<V, ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> {

      SetValueMetasReturnView(MetaParam.Writable[] metas) {
         super(metas);
      }

      @ProtoFactory
      SetValueMetasReturnView(MarshallableArray<MetaParam.Writable> metas) {
         super(metas);
      }

      @Override
      public ReadWriteEntryView<K, V> apply(V v, ReadWriteEntryView<K, V> rw) {
         rw.set(v);
         return rw;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_SET_VALUE_IF_ABSENT_RETURN_PREV_OR_NULL)
   public static final class SetValueIfAbsentReturnPrevOrNull<K, V> implements BiFunction<V, ReadWriteEntryView<K, V>, V> {

      private static final SetValueIfAbsentReturnPrevOrNull INSTANCE = new SetValueIfAbsentReturnPrevOrNull<>();

      @ProtoFactory
      @SuppressWarnings("unchecked")
      static <K, V> SetValueIfAbsentReturnPrevOrNull<K, V> getInstance() {
         return SetValueIfAbsentReturnPrevOrNull.INSTANCE;
      }

      @Override
      public V apply(V v, ReadWriteEntryView<K, V> rw) {
         Optional<V> opt = rw.find();
         V prev = opt.orElse(null);
         if (!opt.isPresent())
            rw.set(v);

         return prev;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_SET_VALUE_METAS_IF_ABSENT_RETURN_PREV_OR_NULL)
   public static final class SetValueMetasIfAbsentReturnPrevOrNull<K, V> extends AbstractParamWritableFunction
         implements BiFunction<V, ReadWriteEntryView<K, V>, V> {

      SetValueMetasIfAbsentReturnPrevOrNull(MetaParam.Writable[] metas) {
         super(metas);
      }

      @ProtoFactory
      SetValueMetasIfAbsentReturnPrevOrNull(MarshallableArray<MetaParam.Writable> metas) {
         super(metas);
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

   @ProtoTypeId(ProtoStreamTypeIds.MF_SET_VALUE_IF_ABSENT_RETURN_BOOLEAN)
   public static final class SetValueIfAbsentReturnBoolean<K, V> implements BiFunction<V, ReadWriteEntryView<K, V>, Boolean> {

      private static final SetValueIfAbsentReturnBoolean INSTANCE = new SetValueIfAbsentReturnBoolean<>();

      @ProtoFactory
      @SuppressWarnings("unchecked")
      static <K, V> SetValueIfAbsentReturnBoolean<K, V> getInstance() {
         return SetValueIfAbsentReturnBoolean.INSTANCE;
      }

      @Override
      public Boolean apply(V v, ReadWriteEntryView<K, V> rw) {
         Optional<V> opt = rw.find();
         boolean success = !opt.isPresent();
         if (success) rw.set(v);
         return success;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_SET_VALUE_METAS_IF_ABSENT_RETURN_BOOLEAN)
   public static final class SetValueMetasIfAbsentReturnBoolean<K, V> extends AbstractParamWritableFunction
         implements BiFunction<V, ReadWriteEntryView<K, V>, Boolean> {
      SetValueMetasIfAbsentReturnBoolean(MetaParam.Writable[] metas) {
         super(metas);
      }

      @ProtoFactory
      SetValueMetasIfAbsentReturnBoolean(MarshallableArray<MetaParam.Writable> metas) {
         super(metas);
      }

      @Override
      public Boolean apply(V v, ReadWriteEntryView<K, V> rw) {
         Optional<V> opt = rw.find();
         boolean success = !opt.isPresent();
         if (success) rw.set(v, metas);
         return success;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_SET_VALUE_IF_PRESENT_RETURN_PREV_OR_NULL)
   public static final class SetValueIfPresentReturnPrevOrNull<K, V> implements BiFunction<V, ReadWriteEntryView<K, V>, V> {
      private static final SetValueIfPresentReturnPrevOrNull INSTANCE = new SetValueIfPresentReturnPrevOrNull<>();

      @ProtoFactory
      @SuppressWarnings("unchecked")
      static <K, V> SetValueIfPresentReturnPrevOrNull<K, V> getInstance() {
         return SetValueIfPresentReturnPrevOrNull.INSTANCE;
      }

      @Override
      public V apply(V v, ReadWriteEntryView<K, V> rw) {
         return rw.find().map(prev -> {
            rw.set(v);
            return prev;
         }).orElse(null);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_SET_VALUE_METAS_IF_PRESENT_RETURN_PREV_OR_NULL)
   public static final class SetValueMetasIfPresentReturnPrevOrNull<K, V> extends AbstractParamWritableFunction
         implements BiFunction<V, ReadWriteEntryView<K, V>, V> {

      SetValueMetasIfPresentReturnPrevOrNull(MetaParam.Writable[] metas) {
         super(metas);
      }

      @ProtoFactory
      SetValueMetasIfPresentReturnPrevOrNull(MarshallableArray<MetaParam.Writable> metas) {
         super(metas);
      }

      @Override
      public V apply(V v, ReadWriteEntryView<K, V> rw) {
         return rw.find().map(prev -> {
            rw.set(v, metas);
            return prev;
         }).orElse(null);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_SET_VALUE_IF_PRESENT_RETURN_BOOLEAN)
   public static final class SetValueIfPresentReturnBoolean<K, V> implements BiFunction<V, ReadWriteEntryView<K, V>, Boolean> {
      private static final SetValueIfPresentReturnBoolean INSTANCE = new SetValueIfPresentReturnBoolean<>();

      @ProtoFactory
      @SuppressWarnings("unchecked")
      static <K, V> SetValueIfPresentReturnBoolean<K, V> getInstance() {
         return SetValueIfPresentReturnBoolean.INSTANCE;
      }

      @Override
      public Boolean apply(V v, ReadWriteEntryView<K, V> rw) {
         return rw.find().map(prev -> {
            rw.set(v);
            return true;
         }).orElse(false);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_SET_VALUE_METAS_IF_PRESENT_RETURN_BOOLEAN)
   public static final class SetValueMetasIfPresentReturnBoolean<K, V> extends AbstractParamWritableFunction
         implements BiFunction<V, ReadWriteEntryView<K, V>, Boolean> {
      SetValueMetasIfPresentReturnBoolean(MetaParam.Writable[] metas) {
         super(metas);
      }

      @ProtoFactory
      SetValueMetasIfPresentReturnBoolean(MarshallableArray<MetaParam.Writable> metas) {
         super(metas);
      }

      @Override
      public Boolean apply(V v, ReadWriteEntryView<K, V> rw) {
         return rw.find().map(prev -> {
            rw.set(v);
            return true;
         }).orElse(false);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_SET_VALUE_IF_EQUALS_RETURN_BOOLEAN)
   public static final class SetValueIfEqualsReturnBoolean<K, V> extends AbstractParamWritableFunction
         implements BiFunction<V, ReadWriteEntryView<K, V>, Boolean> {

      final V oldValue;

      SetValueIfEqualsReturnBoolean(V oldValue, MetaParam.Writable[] metas) {
         super(metas);
         this.oldValue = oldValue;
      }

      @ProtoFactory
      SetValueIfEqualsReturnBoolean(MarshallableObject<V> oldValue, MarshallableArray<MetaParam.Writable> metas) {
         super(metas);
         this.oldValue = MarshallableObject.unwrap(oldValue);
      }

      @ProtoField(2)
      MarshallableObject<V> getOldValue() {
         return MarshallableObject.create(oldValue);
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
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_REMOVE_RETURN_PREV_OR_NULL)
   public static final class RemoveReturnPrevOrNull<K, V> implements Function<ReadWriteEntryView<K, V>, V> {
      @Override
      public V apply(ReadWriteEntryView<K, V> rw) {
         V prev = rw.find().orElse(null);
         rw.remove();
         return prev;
      }

      private static final RemoveReturnPrevOrNull INSTANCE = new RemoveReturnPrevOrNull<>();

      @ProtoFactory
      @SuppressWarnings("unchecked")
      static <K, V> RemoveReturnPrevOrNull<K, V> getInstance() {
         return RemoveReturnPrevOrNull.INSTANCE;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_REMOVE_RETURN_BOOLEAN)
   public static final class RemoveReturnBoolean<K, V> implements Function<ReadWriteEntryView<K, V>, Boolean> {
      @Override
      public Boolean apply(ReadWriteEntryView<K, V> rw) {
         boolean success = rw.find().isPresent();
         rw.remove();
         return success;
      }

      private static final RemoveReturnBoolean INSTANCE = new RemoveReturnBoolean<>();

      @ProtoFactory
      @SuppressWarnings("unchecked")
      static <K, V> RemoveReturnBoolean<K, V> getInstance() {
         return RemoveReturnBoolean.INSTANCE;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_REMOVE_IF_VALUE_EQUALS_RETURN_BOOLEAN)
   public static final class RemoveIfValueEqualsReturnBoolean<K, V> implements BiFunction<V, ReadWriteEntryView<K, V>, Boolean> {
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

      private static final RemoveIfValueEqualsReturnBoolean INSTANCE = new RemoveIfValueEqualsReturnBoolean<>();

      @ProtoFactory
      @SuppressWarnings("unchecked")
      static <K, V> RemoveIfValueEqualsReturnBoolean<K, V> getInstance() {
         return RemoveIfValueEqualsReturnBoolean.INSTANCE;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_SET_VALUE)
   public static final class SetValue<K, V> implements BiConsumer<V, WriteEntryView<K, V>> {
      private static final SetValue INSTANCE = new SetValue<>();

      @ProtoFactory
      @SuppressWarnings("unchecked")
      static <K, V> SetValue<K, V> getInstance() {
         return SetValue.INSTANCE;
      }

      @Override
      public void accept(V v, WriteEntryView<K, V> wo) {
         wo.set(v);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_SET_VALUE_META)
   public static final class SetValueMetas<K, V> extends AbstractParamWritableFunction implements BiConsumer<V, WriteEntryView<K, V>> {
      SetValueMetas(MetaParam.Writable[] metas) {
         super(metas);
      }

      @ProtoFactory
      SetValueMetas(MarshallableArray<MetaParam.Writable> metas) {
         super(metas);
      }

      @Override
      public void accept(V v, WriteEntryView<K, V> wo) {
         wo.set(v, metas);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_SET_INTERNAL_CACHE_VALUE)
   public static final class SetInternalCacheValue<K, V> implements BiConsumer<InternalCacheValue<V>, WriteEntryView<K, V>> {
      private static final SetInternalCacheValue INSTANCE = new SetInternalCacheValue<>();

      @ProtoFactory
      @SuppressWarnings("unchecked")
      static <K, V> SetInternalCacheValue<K, V> getInstance() {
         return SetInternalCacheValue.INSTANCE;
      }

      @Override
      public void accept(InternalCacheValue<V> icv, WriteEntryView<K, V> view) {
         view.set(icv.getValue(), icv.getMetadata());
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_REMOVE)
   public static final class Remove<K, V> implements Consumer<WriteEntryView<K, V>> {
      @Override
      public void accept(WriteEntryView<K, V> wo) {
         wo.remove();
      }

      private static final Remove INSTANCE = new Remove<>();

      @ProtoFactory
      @SuppressWarnings("unchecked")
      static <K, V> Remove<K, V> getInstance() {
         return Remove.INSTANCE;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_RETURN_READ_WRITE_FIND)
   public static final class ReturnReadWriteFind<K, V> implements Function<ReadWriteEntryView<K, V>, Optional<V>> {
      @Override
      public Optional<V> apply(ReadWriteEntryView<K, V> rw) {
         return rw.find();
      }

      private static final ReturnReadWriteFind INSTANCE = new ReturnReadWriteFind<>();

      @ProtoFactory
      @SuppressWarnings("unchecked")
      static <K, V> ReturnReadWriteFind<K, V> getInstance() {
         return ReturnReadWriteFind.INSTANCE;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_RETURN_READ_WRITE_GET)
   public static final class ReturnReadWriteGet<K, V> implements Function<ReadWriteEntryView<K, V>, V> {
      @Override
      public V apply(ReadWriteEntryView<K, V> rw) {
         return rw.get();
      }

      private static final ReturnReadWriteGet INSTANCE = new ReturnReadWriteGet<>();

      @ProtoFactory
      @SuppressWarnings("unchecked")
      static <K, V> ReturnReadWriteGet<K, V> getInstance() {
         return ReturnReadWriteGet.INSTANCE;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_RETURN_READ_WRITE_VIEW)
   public static final class ReturnReadWriteView<K, V> implements Function<ReadWriteEntryView<K, V>, ReadWriteEntryView<K, V>> {
      @Override
      public ReadWriteEntryView<K, V> apply(ReadWriteEntryView<K, V> rw) {
         return rw;
      }

      private static final ReturnReadWriteView INSTANCE = new ReturnReadWriteView<>();

      @ProtoFactory
      @SuppressWarnings("unchecked")
      static <K, V> ReturnReadWriteView<K, V> getInstance() {
         return ReturnReadWriteView.INSTANCE;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_RETURN_READ_ONLY_FIND_OR_NULL)
   public static final class ReturnReadOnlyFindOrNull<K, V> implements Function<ReadEntryView<K, V>, V> {
      @Override
      public V apply(ReadEntryView<K, V> ro) {
         return ro.find().orElse(null);
      }

      private static final ReturnReadOnlyFindOrNull INSTANCE = new ReturnReadOnlyFindOrNull<>();

      @ProtoFactory
      @SuppressWarnings("unchecked")
      static <K, V> ReturnReadOnlyFindOrNull<K, V> getInstance() {
         return INSTANCE;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_RETURN_READ_ONLY_FIND_IS_PRESENT)
   public static final class ReturnReadOnlyFindIsPresent<K, V> implements Function<ReadEntryView<K, V>, Boolean> {
      @Override
      public Boolean apply(ReadEntryView<K, V> ro) {
         return ro.find().isPresent();
      }

      private static final ReturnReadOnlyFindIsPresent INSTANCE = new ReturnReadOnlyFindIsPresent<>();

      @ProtoFactory
      @SuppressWarnings("unchecked")
      static <K, V> ReturnReadOnlyFindIsPresent<K, V> getInstance() {
         return INSTANCE;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MF_IDENTITY)
   public static final class Identity<T> implements InjectiveFunction<T, T>, UnaryOperator<T> {
      @Override
      public T apply(T o) {
         return o;
      }

      private static final Identity INSTANCE = new Identity<>();

      @ProtoFactory
      static <T> Identity<T> getInstance() {
         return INSTANCE;
      }
   }

   private MarshallableFunctions() {
      // No-op, holds static variables
   }
}
