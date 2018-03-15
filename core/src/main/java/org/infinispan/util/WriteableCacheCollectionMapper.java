package org.infinispan.util;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.CacheCollection;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorMapper;
import org.infinispan.commons.util.InjectiveFunction;

/**
 * A writeable cache collection mapper that also has constant time operations for things such as
 * {@link Collection#contains(Object)} if the underlying Collection does.
 * <p>
 * This collection should be used for cases when a simple transformation of a element to another is all that is
 * needed by the underlying collection.
 * <p>
 * Note this class allows for a different function specifically for values returned from an iterator. This
 * can be useful to intercept calls such as {@link java.util.Map.Entry#setValue(Object)} and update appropriately.
 * @author wburns
 * @since 9.2
 */
public class WriteableCacheCollectionMapper<E, R> extends CacheCollectionMapper<E, R> {
   protected final Function<? super E, ? extends R> toNewTypeIteratorFunction;
   protected final Function<? super R, ? extends E> fromNewTypeFunction;

   public WriteableCacheCollectionMapper(CacheCollection<E> realCollection,
         Function<? super E, ? extends R> toNewTypeFunction,
         Function<? super R, ? extends E> fromNewTypeFunction,
         InjectiveFunction<Object, ?> keyFilterFunction) {
      super(realCollection, toNewTypeFunction, keyFilterFunction);
      this.toNewTypeIteratorFunction = toNewTypeFunction;
      this.fromNewTypeFunction = fromNewTypeFunction;
   }

   public WriteableCacheCollectionMapper(CacheCollection<E> realCollection,
         Function<? super E, ? extends R> toNewTypeFunction,
         Function<? super E, ? extends R> toNewTypeIteratorFunction,
         Function<? super R, ? extends E> fromNewTypeFunction,
         InjectiveFunction<Object, ?> keyFilterFunction) {
      super(realCollection, toNewTypeFunction, keyFilterFunction);
      this.toNewTypeIteratorFunction = toNewTypeIteratorFunction;
      this.fromNewTypeFunction = fromNewTypeFunction;
   }

   @Override
   public CloseableIterator<R> iterator() {
      return new CloseableIteratorMapper<>(realCollection.iterator(), toNewTypeIteratorFunction);
   }

   @Override
   public boolean contains(Object o) {
      return realCollection.contains(fromNewTypeFunction.apply((R) o));
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      return realCollection.containsAll(new CollectionMapper<>((Collection<R>) c, fromNewTypeFunction));
   }

   @Override
   public boolean add(R e) {
      return realCollection.add(fromNewTypeFunction.apply(e));
   }

   @Override
   public boolean addAll(Collection<? extends R> c) {
      return realCollection.addAll(new CollectionMapper<>((Collection<R>) c, fromNewTypeFunction));
   }

   @Override
   public boolean remove(Object o) {
      return realCollection.remove(fromNewTypeFunction.apply((R) o));
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      return realCollection.removeAll(new CollectionMapper<>((Collection<R>) c, fromNewTypeFunction));
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      return realCollection.retainAll(new CollectionMapper<>((Collection<R>) c, fromNewTypeFunction));
   }

   @Override
   public boolean removeIf(Predicate<? super R> filter) {
      return realCollection.removeIf(e -> filter.test(mapper.apply(e)));
   }

   @Override
   public void clear() {
      realCollection.clear();
   }
}
