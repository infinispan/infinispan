package org.infinispan.commons.util;

import java.util.function.Function;

/**
 * A spliterator that maps each value to the output of the Function that is also closeable.
 * @author wburns
 * @since 9.0
 */
public class CloseableSpliteratorMapper<E, S> extends SpliteratorMapper<E, S> implements CloseableSpliterator<S> {
   private final CloseableSpliterator<? extends E> spliterator;

   public CloseableSpliteratorMapper(CloseableSpliterator<E> spliterator, Function<? super E, ? extends S> mapper) {
      super(spliterator, mapper);
      this.spliterator = spliterator;
   }

   @Override
   public void close() {
      spliterator.close();
   }
}
