package org.infinispan.util;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorMapper;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.CloseableSpliteratorMapper;

import java.util.function.Function;

/**
 * A {@link CacheCollection} that allows for a different set to be mapped as
 * a different instance with values replaced on request.  This is useful as a
 * cache collection that is normally lazily evaluated to prevent having to
 * pull all values into memory which can be a lot faster when checking single
 * values and can also prevent out of memory issues.
 *
 * @param <E> Type of elements in collection before transformation
 * @param <R> Type of elements in collection after transformation
 * @since 9.0
 */
public class CacheCollectionMapper<E, R> extends CollectionMapper<E, R> implements CacheCollection<R> {
   protected final CacheCollection<E> realCacheCollection;

   public CacheCollectionMapper(CacheCollection<E> realCollection, Function<? super E, ? extends R> mapper) {
      super(realCollection, mapper);
      this.realCacheCollection = realCollection;
   }

   @Override
   public CloseableSpliterator<R> spliterator() {
      return new CloseableSpliteratorMapper<>(realCacheCollection.spliterator(), mapper);
   }

   @Override
   public CloseableIterator<R> iterator() {
      return new CloseableIteratorMapper<>(realCacheCollection.iterator(), mapper);
   }

   @Override
   public CacheStream<R> stream() {
      return realCacheCollection.stream().map(mapper);
   }

   @Override
   public CacheStream<R> parallelStream() {
      return realCacheCollection.parallelStream().map(mapper);
   }

}
