package org.infinispan.stream.impl;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.util.AbstractDelegatingCloseableIteratorCollection;

/**
 *
 * @param <E>
 */
public abstract class AbstractDelegatingCacheCollection<E> extends AbstractDelegatingCloseableIteratorCollection<E>
        implements CacheCollection<E> {
   @Override
   protected abstract CacheCollection<E> delegate();

   @Override
   public abstract CacheStream<E> stream();

   @Override
   public abstract CacheStream<E> parallelStream();
}
