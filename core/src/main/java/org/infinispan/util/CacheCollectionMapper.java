package org.infinispan.util;

import java.util.Collection;
import java.util.function.Function;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.InjectiveFunction;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.SpliteratorMapper;

/**
 * A {@link CacheCollection} that allows for a different set to be mapped as
 * a different instance with values replaced on request.  This is useful as a
 * cache collection that is normally lazily evaluated to prevent having to
 * pull all values into memory which can be a lot faster when checking single
 * values and can also prevent out of memory issues.
 * <p>
 * Some operations such as {@link Collection#contains(Object)} and {@link Collection#containsAll(Collection)} may be
 * more expensive then normal since they cannot utilize lookups into the original collection.
 * @param <E> Type of elements in collection before transformation
 * @param <R> Type of elements in collection after transformation
 * @since 9.0
 * @deprecated since 9.2.1 It is recommended to use {@link WriteableCacheCollectionMapper} instead as it allows for
 * constant time contains and other operations
 */
@Deprecated
public class CacheCollectionMapper<E, R> extends CollectionMapper<E, R> implements CacheCollection<R> {
   protected final CacheCollection<E> realCacheCollection;
   protected final InjectiveFunction<Object, ?> keyFilterMapper;

   /**
    * CacheCollection that maps entries to new type that takes a key filter that is {@link Function#identity()}
    * @param realCollection the collection storing original entries
    * @param mapper the mapper to the new type
    */
   public CacheCollectionMapper(CacheCollection<E> realCollection, Function<? super E, ? extends R> mapper) {
      super(realCollection, mapper);
      this.realCacheCollection = realCollection;
      this.keyFilterMapper = f -> f;
   }

   /**
    * CacheCollection that maps entries to new type that takes a provided key filter.
    * @param realCollection the collection storing original entries
    * @param mapper the mapper to the new type
    * @param keyFilterMapper the key filter mapper to use (since collection may not be the same type)
    */
   public CacheCollectionMapper(CacheCollection<E> realCollection, Function<? super E, ? extends R> mapper,
         InjectiveFunction<Object, ?> keyFilterMapper) {
      super(realCollection, mapper);
      this.realCacheCollection = realCollection;
      this.keyFilterMapper = keyFilterMapper;
   }

   @Override
   public CloseableSpliterator<R> spliterator() {
      return new SpliteratorMapper<>(realCacheCollection.spliterator(), mapper);
   }

   @Override
   public CloseableIterator<R> iterator() {
      return new IteratorMapper<>(realCacheCollection.iterator(), mapper);
   }

   private CacheStream<R> getStream(CacheStream<E> parentStream) {
      return new MappedCacheStream<>(parentStream.map(mapper), keyFilterMapper);
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
