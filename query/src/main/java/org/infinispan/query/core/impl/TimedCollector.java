package org.infinispan.query.core.impl;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.time.DefaultTimeService;
import org.infinispan.commons.time.TimeService;

/**
 * @since 11.0
 */
class TimedCollector<U, A, R> implements Collector<U, A, R> {
   private final Collector<U, A, R> collector;
   private final long timeout;

   private static final TimeService TIME_SERVICE = DefaultTimeService.INSTANCE;

   public TimedCollector(Collector<U, A, R> collector, long timeout) {
      this.collector = collector;
      this.timeout = timeout;
   }

   @Override
   public Supplier<A> supplier() {
      return collector.supplier();
   }

   @Override
   public BiConsumer<A, U> accumulator() {
      BiConsumer<A, U> accumulator = collector.accumulator();

      if (timeout < 0) return accumulator;

      return new BiConsumer<A, U>() {
         int index = 0;
         final long limit = TIME_SERVICE.time() + timeout;

         boolean divBy32(int i) {
            return (i & ((1 << 5) - 1)) == 0;
         }

         @Override
         public void accept(A a, U u) {
            if (divBy32(index++) && TIME_SERVICE.isTimeExpired(limit)) {
               throw new TimeoutException();
            }
            accumulator.accept(a, u);
         }
      };
   }

   @Override
   public BinaryOperator<A> combiner() {
      return collector.combiner();
   }

   @Override
   public Function<A, R> finisher() {
      return collector.finisher();
   }

   @Override
   public Set<Characteristics> characteristics() {
      return collector.characteristics();
   }
}
