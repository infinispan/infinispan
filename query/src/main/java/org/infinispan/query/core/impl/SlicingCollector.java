package org.infinispan.query.core.impl;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Delegates to the underlying collector only if item is inside the supplied window, while still keeping the count
 * of all items of the stream.
 *
 * @since 12.1
 */
class SlicingCollector<U, A, R> implements Collector<U, A, R> {
   private final Collector<U, A, R> collector;
   private final long skip;
   private final long limit;
   private long count = 0;

   public SlicingCollector(Collector<U, A, R> collector, long skip, long limit) {
      this.collector = collector;
      this.skip = skip;
      this.limit = limit < 0 ? -1 : limit;
   }

   long getCount() {
      return count;
   }

   @Override
   public Supplier<A> supplier() {
      return collector.supplier();
   }

   @Override
   public BiConsumer<A, U> accumulator() {
      BiConsumer<A, U> accumulator = collector.accumulator();
      return (a, u) -> {
         count++;
         if (count > skip && (limit == -1 || count <= skip + limit))
            accumulator.accept(a, u);
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
