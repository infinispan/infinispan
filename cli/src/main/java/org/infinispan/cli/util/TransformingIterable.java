package org.infinispan.cli.util;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * @since 14.0
 **/
public class TransformingIterable<S, T> implements Iterable<T> {

   public static Function<Map<String, String>, String> SINGLETON_MAP_VALUE = m -> m.values().iterator().next();

   private final Iterable<S> iterable;
   private final Function<S, T> transformer;

   public TransformingIterable(Iterable<S> iterable, Function<S, T> transformer) {
      this.iterable = iterable;
      this.transformer = transformer;
   }

   @Override
   public Iterator<T> iterator() {
      return new TransformingIterator<>(iterable.iterator(), transformer);
   }
}
