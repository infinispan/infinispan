package org.infinispan.stream.impl;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.ObjLongConsumer;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class CacheBiConsumers {
   private CacheBiConsumers() { }

   static <K, V, R> Consumer<R> objectConsumer(BiConsumer<Cache<K, V>, ? super R> biConsumer) {
      return new CacheObjBiConsumer<>(biConsumer);
   }

   static <K, V> DoubleConsumer doubleConsumer(ObjDoubleConsumer<Cache<K, V>> objDoubleConsumer) {
      return new CacheDoubleConsumer<>(objDoubleConsumer);
   }

   static <K, V> LongConsumer longConsumer(ObjLongConsumer<Cache<K, V>> objLongConsumer) {
      return new CacheLongConsumer<>(objLongConsumer);
   }

   static <K, V> IntConsumer intConsumer(ObjIntConsumer<Cache<K, V>> objIntConsumer) {
      return new CacheIntConsumer<>(objIntConsumer);
   }

   @Scope(Scopes.NONE)
   public static class CacheObjBiConsumer<K, V, R> implements Consumer<R> {
      private final BiConsumer<Cache<K, V>, ? super R> biConsumer;

      protected transient Cache<K, V> cache;

      @Inject
      void inject(Cache<K, V> cache, ComponentRegistry componentRegistry) {
         componentRegistry.wireDependencies(biConsumer);
         this.cache = cache;
      }

      CacheObjBiConsumer(BiConsumer<Cache<K, V>, ? super R> biConsumer) {
         this.biConsumer = biConsumer;
      }

      @ProtoFactory
      CacheObjBiConsumer(MarshallableObject<BiConsumer<Cache<K, V>, ? super R>> consumer) {
         this.biConsumer = MarshallableObject.unwrap(consumer);
      }

      @ProtoField(number = 1)
      MarshallableObject<BiConsumer<Cache<K, V>, ? super R>> getConsumer() {
         return MarshallableObject.create(biConsumer);
      }

      @Override
      public void accept(R r) {
         biConsumer.accept(cache, r);
      }
   }

   @Scope(Scopes.NONE)
   public static class CacheDoubleConsumer<K, V> implements DoubleConsumer {
      private final ObjDoubleConsumer<Cache<K, V>> objDoubleConsumer;

      protected transient Cache<K, V> cache;

      @Inject
      void inject(Cache<K, V> cache, ComponentRegistry componentRegistry) {
         componentRegistry.wireDependencies(objDoubleConsumer);
         this.cache = cache;
      }

      CacheDoubleConsumer(ObjDoubleConsumer<Cache<K, V>> objDoubleConsumer) {
         this.objDoubleConsumer = objDoubleConsumer;
      }

      @ProtoFactory
      CacheDoubleConsumer(MarshallableObject<ObjDoubleConsumer<Cache<K, V>>> consumer) {
         this.objDoubleConsumer = MarshallableObject.unwrap(consumer);
      }

      @ProtoField(number = 1)
      MarshallableObject<ObjDoubleConsumer<Cache<K, V>>> getConsumer() {
         return MarshallableObject.create(objDoubleConsumer);
      }

      @Override
      public void accept(double r) {
         objDoubleConsumer.accept(cache, r);
      }
   }

   @Scope(Scopes.NONE)
   public static class CacheLongConsumer<K, V> implements LongConsumer {
      private final ObjLongConsumer<Cache<K, V>> objLongConsumer;

      protected transient Cache<K, V> cache;

      @Inject
      void inject(Cache<K, V> cache, ComponentRegistry componentRegistry) {
         componentRegistry.wireDependencies(objLongConsumer);
         this.cache = cache;
      }

      CacheLongConsumer(ObjLongConsumer<Cache<K, V>> objLongConsumer) {
         this.objLongConsumer = objLongConsumer;
      }

      @ProtoFactory
      CacheLongConsumer(MarshallableObject<ObjLongConsumer<Cache<K, V>>> consumer) {
         this.objLongConsumer = MarshallableObject.unwrap(consumer);
      }

      @ProtoField(number = 1)
      MarshallableObject<ObjLongConsumer<Cache<K, V>>> getConsumer() {
         return MarshallableObject.create(objLongConsumer);
      }

      @Override
      public void accept(long r) {
         objLongConsumer.accept(cache, r);
      }
   }

   @Scope(Scopes.NONE)
   public static class CacheIntConsumer<K, V> implements IntConsumer {
      private final ObjIntConsumer<Cache<K, V>> objIntConsumer;

      protected transient Cache<K, V> cache;

      @Inject
      void inject(Cache<K, V> cache, ComponentRegistry componentRegistry) {
         componentRegistry.wireDependencies(objIntConsumer);
         this.cache = cache;
      }

      CacheIntConsumer(ObjIntConsumer<Cache<K, V>> objIntConsumer) {
         this.objIntConsumer = objIntConsumer;
      }

      @ProtoFactory
      CacheIntConsumer(MarshallableObject<ObjIntConsumer<Cache<K, V>>> consumer) {
         this.objIntConsumer = MarshallableObject.unwrap(consumer);
      }

      @ProtoField(number = 1)
      MarshallableObject<ObjIntConsumer<Cache<K, V>>> getConsumer() {
         return MarshallableObject.create(objIntConsumer);
      }

      @Override
      public void accept(int r) {
         objIntConsumer.accept(cache, r);
      }
   }
}
