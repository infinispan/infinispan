package org.infinispan.stream.impl;

import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.util.AbstractDelegatingCloseableIteratorSet;

/**
 * Same as {@link AbstractDelegatingCacheCollection} except this method implements Set as well.
 * @param <E>
 */
public abstract class AbstractDelegatingCacheSet<E> extends AbstractDelegatingCacheCollection<E>
        implements CacheSet<E> {
   @Override
   protected abstract CacheSet<E> delegate();
}
