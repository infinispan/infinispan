package org.infinispan.stream.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.ObjLongConsumer;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.Ids;

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
   static class CacheObjBiConsumer<K, V, R> implements Consumer<R> {
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

      @Override
      public void accept(R r) {
         biConsumer.accept(cache, r);
      }
   }

   @Scope(Scopes.NONE)
   static class CacheDoubleConsumer<K, V> implements DoubleConsumer {
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

      @Override
      public void accept(double r) {
         objDoubleConsumer.accept(cache, r);
      }
   }

   @Scope(Scopes.NONE)
   static class CacheLongConsumer<K, V> implements LongConsumer {
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

      @Override
      public void accept(long r) {
         objLongConsumer.accept(cache, r);
      }
   }

   @Scope(Scopes.NONE)
   static class CacheIntConsumer<K, V> implements IntConsumer {
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

      @Override
      public void accept(int r) {
         objIntConsumer.accept(cache, r);
      }
   }

   public static class Externalizer implements AdvancedExternalizer<Object> {

      enum ExternalizerId {
         OBJECT(CacheObjBiConsumer.class),
         DOUBLE(CacheDoubleConsumer.class),
         LONG(CacheLongConsumer.class),
         INT(CacheIntConsumer.class)
         ;

         private final Class<?> marshalledClass;

         ExternalizerId(Class<?> marshalledClass) {
            this.marshalledClass = marshalledClass;
         }
      }

      private static final ExternalizerId[] VALUES = ExternalizerId.values();

      private final Map<Class<?>, ExternalizerId> objects = new HashMap<>();

      public Externalizer() {
         for (ExternalizerId id : VALUES) {
            objects.put(id.marshalledClass, id);
         }
      }

      @Override
      public Set<Class<?>> getTypeClasses() {
         return Util.asSet(CacheObjBiConsumer.class, CacheDoubleConsumer.class, CacheLongConsumer.class,
               CacheIntConsumer.class);
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_BI_CONSUMERS;
      }

      @Override
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         ExternalizerId id = objects.get(object.getClass());
         if (id == null) {
            throw new IllegalArgumentException("Unsupported class " + object.getClass() + " was provided!");
         }
         output.writeByte(id.ordinal());
         switch (id) {
            case OBJECT:
               output.writeObject(((CacheObjBiConsumer) object).biConsumer);
               break;
            case DOUBLE:
               output.writeObject(((CacheDoubleConsumer) object).objDoubleConsumer);
               break;
            case LONG:
               output.writeObject(((CacheLongConsumer) object).objLongConsumer);
               break;
            case INT:
               output.writeObject(((CacheIntConsumer) object).objIntConsumer);
               break;
         }
      }

      @Override
      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int number = input.readUnsignedByte();
         ExternalizerId[] ids = VALUES;
         if (number >= ids.length) {
            throw new IllegalArgumentException("Found invalid number " + number);
         }
         ExternalizerId id = ids[number];
         switch (id) {
            case OBJECT:
               return new CacheObjBiConsumer<>((BiConsumer) input.readObject());
            case DOUBLE:
               return new CacheDoubleConsumer<>((ObjDoubleConsumer) input.readObject());
            case LONG:
               return new CacheLongConsumer<>((ObjLongConsumer) input.readObject());
            case INT:
               return new CacheIntConsumer<>((ObjIntConsumer) input.readObject());
            default:
               throw new IllegalArgumentException("ExternalizerId not supported: " + id);
         }
      }
   }
}
