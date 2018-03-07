package org.infinispan.util;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;

/**
 *
 * @param <E>
 */
public abstract class AbstractDelegatingCacheCollection<E> extends AbstractDelegatingCloseableIteratorCollection<E>
        implements CacheCollection<E> {
   @Override
   protected abstract CacheCollection<E> delegate();

   @Override
   public CacheStream<E> stream() {
      return delegate().stream();
   }

   @Override
   public CacheStream<E> parallelStream() {
      return delegate().parallelStream();
   }
}
