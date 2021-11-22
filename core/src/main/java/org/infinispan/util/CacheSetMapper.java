package org.infinispan.util;

import java.util.function.Function;

import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.SpliteratorMapper;

/**
 * A {@link CacheSet} that allows for a different set to be mapped as a different instance wtih values replaced on
 * request.  This is useful as a cache set is normally lazily evaluated to prevent having to pull all values into memory
 * which can be a lot faster when checking single values and can also prevent out of memory issues.
 * @author wburns
 * @since 9.0
 */
public class CacheSetMapper<E, R> extends SetMapper<E, R> implements CacheSet<R> {
   protected final CacheSet<E> realSet;

   public CacheSetMapper(CacheSet<E> realSet, Function<? super E, ? extends R> mapper) {
      super(realSet, mapper);
      this.realSet = realSet;
   }

   @Override
   public CacheStream<R> stream() {
      return realSet.stream().map(mapper);
   }

   @Override
   public CacheStream<R> parallelStream() {
      return realSet.parallelStream().map(mapper);
   }

   @Override
   public CloseableSpliterator<R> spliterator() {
      return new SpliteratorMapper<>(realSet.spliterator(), mapper);
   }

   @Override
   public CloseableIterator<R> iterator() {
      return new IteratorMapper<>(realSet.iterator(), mapper);
   }
}
