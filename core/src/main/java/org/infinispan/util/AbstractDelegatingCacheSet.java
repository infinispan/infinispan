package org.infinispan.util;

import org.infinispan.CacheSet;

/**
 * Same as {@link AbstractDelegatingCacheCollection} except this method implements Set as well.
 * @param <E>
 */
public abstract class AbstractDelegatingCacheSet<E> extends AbstractDelegatingCacheCollection<E>
        implements CacheSet<E> {
   @Override
   protected abstract CacheSet<E> delegate();
}
