package org.infinispan;

import org.infinispan.commons.util.CloseableIteratorSet;

import java.util.stream.Stream;

/**
 * A set that also must implement the various {@link CacheCollection} methods for streams.
 * @param <E> The type of the set
 */
public interface CacheSet<E> extends CacheCollection<E>, CloseableIteratorSet<E> {
}
