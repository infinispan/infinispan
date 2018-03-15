package org.infinispan.util;

import java.util.function.Function;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorMapper;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.CloseableSpliteratorMapper;
import org.infinispan.commons.util.InjectiveFunction;

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
   protected final InjectiveFunction<Object, ?> keyFilterMapper;

   public CacheCollectionMapper(CacheCollection<E> realCollection, Function<? super E, ? extends R> mapper,
         InjectiveFunction<Object, ?> keyFilterMapper) {
      super(realCollection, mapper);
      this.realCacheCollection = realCollection;
      this.keyFilterMapper = keyFilterMapper;
   }

   @Override
   public CloseableSpliterator<R> spliterator() {
      return new CloseableSpliteratorMapper<>(realCacheCollection.spliterator(), mapper);
   }

   @Override
   public CloseableIterator<R> iterator() {
      return new CloseableIteratorMapper<>(realCacheCollection.iterator(), mapper);
   }

   private CacheStream<R> getStream(CacheStream<E> parentStream) {
      return new CacheStreamMapper<>(parentStream.map(mapper), keyFilterMapper);
   }

   @Override
   public CacheStream<R> stream() {
      return getStream(realCacheCollection.stream());
   }

   @Override
   public CacheStream<R> parallelStream() {
      return getStream(realCacheCollection.parallelStream());
   }

}
