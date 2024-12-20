package org.infinispan.stream;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableSupplier;

/**
 * Helper class designed to be used to create a serializable Collector for use with
 * {@link org.infinispan.CacheStream#collect(Collector)} from a supplier of a collector.  The problem is that the
 * standard {@link java.util.stream.Collectors} class doesn't provide Serializable Collectors and no way to extend
 * their functionality, so this class is used instead.
 */
public class CacheCollectors {
   private CacheCollectors() { }

   /**
    * Creates a collector that is serializable and will upon usage create a collector using the serializable supplier
    * provided by the user.
    * @param supplier The supplier to crate the collector that is specifically serializable
    * @param <T> The input type of the collector
    * @param <R> The resulting type of the collector
    * @return the collector which is serializable
    * @see SerializableSupplier
    */
   public static <T, R> Collector<T, ?, R> serializableCollector(SerializableSupplier<Collector<T, ?, R>> supplier) {
      return new CollectorSupplier<>(supplier);
   }

   /**
    * Similar to {@link CacheCollectors#serializableCollector(SerializableSupplier)} except that the supplier provided
    * must be marshable through ISPN marshalling techniques.  Note this is not detected until runtime.
    * @param supplier The marshallable supplier of collectors
    * @param <T> The input type of the collector
    * @param <R> The resulting type of the collector
    * @return the collector which is serializable
    */
   public static <T, R> Collector<T, ?, R> collector(Supplier<Collector<T, ?, R>> supplier) {
      return new CollectorSupplier<>(supplier);
   }

   @ProtoTypeId(ProtoStreamTypeIds.CACHE_COLLECTORS_COLLECTOR_SUPPLIER)
   public static final class CollectorSupplier<T, R> implements Collector<T, Object, R> {
      private final Supplier<Collector<T, ?, R>> supplier;
      private transient Collector<T, Object, R> collector;

      private Collector<T, Object, R> getCollector() {
         if (collector == null) {
            collector = (Collector<T, Object, R>) supplier.get();
         }
         return collector;
      }

      CollectorSupplier(Supplier<Collector<T, ?, R>> supplier) {
         this.supplier = supplier;
      }

      @ProtoFactory
      CollectorSupplier(MarshallableObject<Supplier<Collector<T, ?, R>>> supplier) {
         this.supplier = MarshallableObject.unwrap(supplier);
      }

      @ProtoField(number = 1)
      MarshallableObject<Supplier<Collector<T, ?, R>>> getSupplier() {
         return MarshallableObject.create(supplier);
      }

      @Override
      public Supplier<Object> supplier() {
         return getCollector().supplier();
      }

      @Override
      public BiConsumer<Object, T> accumulator() {
         return getCollector().accumulator();
      }

      @Override
      public BinaryOperator<Object> combiner() {
         return getCollector().combiner();
      }

      @Override
      public Function<Object, R> finisher() {
         return getCollector().finisher();
      }

      @Override
      public Set<Characteristics> characteristics() {
         return getCollector().characteristics();
      }
   }
}
