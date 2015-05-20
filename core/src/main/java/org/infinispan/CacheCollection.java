package org.infinispan;

import org.infinispan.commons.util.CloseableIteratorCollection;

/**
 * A collection type that returns special Cache based streams that have additional options to tweak behavior.
 * @param <E> The type of the collection
 * @since 8.0
 */
public interface CacheCollection<E> extends CloseableIteratorCollection<E> {
   @Override
   CacheStream<E> stream();

   @Override
   CacheStream<E> parallelStream();
}
