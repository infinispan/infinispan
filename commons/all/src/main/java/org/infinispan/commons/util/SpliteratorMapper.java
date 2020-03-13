package org.infinispan.commons.util;

import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A spliterator that has been mapped from another spliterator.  This is nice to only lazily convert these values, so
 * that you can convert across multiple threads or if the entire spliterator is not consumed.
 * <p>
 * This spliterator will <b>always</b> throw an {@link IllegalStateException} upon invocation of
 * {@link Spliterator#getComparator()} since there is no trivial way of converting this with a mapper.
 * @author wburns
 * @since 9.0
 */
public class SpliteratorMapper<E, S> implements CloseableSpliterator<S> {
   protected final Spliterator<E> spliterator;
   protected final Function<? super E, ? extends S> mapper;

   public SpliteratorMapper(Spliterator<E> spliterator, Function<? super E, ? extends S> mapper) {
      this.spliterator = Objects.requireNonNull(spliterator);
      this.mapper = Objects.requireNonNull(mapper);
   }

   @Override
   public boolean tryAdvance(Consumer<? super S> action) {
      return spliterator.tryAdvance(e -> action.accept(mapper.apply(e)));
   }

   @Override
   public Spliterator<S> trySplit() {
      Spliterator<E> split = spliterator.trySplit();
      if (split != null) {
         return new SpliteratorMapper<>(split, mapper);
      }
      return null;
   }

   @Override
   public long estimateSize() {
      return spliterator.estimateSize();
   }

   @Override
   public int characteristics() {
      int characteristics = spliterator.characteristics();
      if (mapper instanceof InjectiveFunction) {
         return characteristics;
      } else {
         // Have to unset distinct if the function wasn't distinct
         return characteristics & ~Spliterator.DISTINCT;
      }
   }

   @Override
   public void forEachRemaining(Consumer<? super S> action) {
      spliterator.forEachRemaining(e -> action.accept(mapper.apply(e)));
   }

   @Override
   public long getExactSizeIfKnown() {
      return spliterator.getExactSizeIfKnown();
   }

   @Override
   public boolean hasCharacteristics(int characteristics) {
      return spliterator.hasCharacteristics(characteristics);
   }

   @Override
   public void close() {
      if (spliterator instanceof CloseableSpliterator) {
         ((CloseableSpliterator) spliterator).close();
      }
   }
}
