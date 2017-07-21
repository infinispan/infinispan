package org.infinispan.commons.util;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class ArrayCollector implements java.util.stream.Collector<Object, ArrayCollector, ArrayCollector>, Supplier<ArrayCollector> {
   private final Object[] array;
   private int pos = 0;

   public ArrayCollector(Object[] array) {
      this.array = array;
   }

   public void add(Object item) {
      array[pos] = item;
      ++pos;
   }

   @Override
   public Supplier<ArrayCollector> supplier() {
      return this;
   }

   @Override
   public ArrayCollector get() {
      return this;
   }

   @Override
   public BiConsumer<ArrayCollector, Object> accumulator() {
      return ArrayCollector::add;
   }

   @Override
   public BinaryOperator<ArrayCollector> combiner() {
      return (a1, a2) -> {
         throw new UnsupportedOperationException("The stream is not supposed to be parallel");
      };
   }

   @Override
   public Function<ArrayCollector, ArrayCollector> finisher() {
      return Function.identity();
   }

   @Override
   public Set<Characteristics> characteristics() {
      return EnumSet.of(Characteristics.IDENTITY_FINISH);
   }
}
