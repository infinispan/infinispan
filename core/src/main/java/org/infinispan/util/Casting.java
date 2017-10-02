package org.infinispan.util;

import org.infinispan.util.function.SerializableSupplier;

import java.util.function.Supplier;
import java.util.stream.Collector;

public class Casting {

   // The hacks here allow casts to work properly,
   // since Java doesn't work as well with nested generics

   @SuppressWarnings("unchecked")
   public static <T, R> SerializableSupplier<Collector<T, ?, R>> toSerialSupplierCollect(
      SerializableSupplier supplier) {
      return supplier;
   }

   // This is a hack to allow for cast to work properly, since Java doesn't work as well with nested generics
   @SuppressWarnings("unchecked")
   public static <T, R> Supplier<Collector<T, ?, R>> toSupplierCollect(Supplier supplier) {
      return supplier;
   }

}
