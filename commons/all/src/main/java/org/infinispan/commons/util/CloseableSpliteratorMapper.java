package org.infinispan.commons.util;

import java.util.Spliterator;
import java.util.function.Function;

/**
 * A spliterator that maps each value to the output of the Function and is also closeable. If the underlying
 * spliterator is closeable, it will also close it
 * @author wburns
 * @since 9.0
 * @deprecated since 9.3 users can just use {@link SpliteratorMapper} as it handles CloseableSpliterators now
 */
@Deprecated
public class CloseableSpliteratorMapper<E, S> extends SpliteratorMapper<E, S> implements CloseableSpliterator<S> {
   public CloseableSpliteratorMapper(Spliterator<E> spliterator, Function<? super E, ? extends S> mapper) {
      super(spliterator, mapper);
   }

   @Override
   public Spliterator<S> trySplit() {
      Spliterator<E> split = spliterator.trySplit();
      if (split != null) {
         return new CloseableSpliteratorMapper<>(split, mapper);
      }
      return null;
   }

   @Override
   public void close() {
      if (spliterator instanceof CloseableSpliterator) {
         ((CloseableSpliterator) spliterator).close();
      }
   }
}
