package org.infinispan.util;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.SpliteratorMapper;

/**
 * A collection that maps another one to a new one of a possibly different type.  Note this collection is read only
 * and doesn't accept write operations.
 * <p>
 * Some operations such as {@link Collection#contains(Object)} and {@link Collection#containsAll(Collection)} may be
 * more expensive then normal since they cannot utilize lookups into the original collection.
 * @author wburns
 * @since 9.0
 */
public class CollectionMapper<E, R> extends AbstractCollection<R> {
   protected final Collection<E> realCollection;
   protected final Function<? super E, ? extends R> mapper;

   public CollectionMapper(Collection<E> realCollection, Function<? super E, ? extends R> mapper) {
      this.realCollection = realCollection;
      this.mapper = mapper;
   }

   @Override
   public int size() {
      return realCollection.size();
   }

   @Override
   public boolean isEmpty() {
      return realCollection.isEmpty();
   }

   @Override
   public Iterator<R> iterator() {
      return new IteratorMapper<E, R>(realCollection.iterator(), mapper) {
         @Override
         public void remove() {
            throw new UnsupportedOperationException("remove");
         }
      };
   }

   @Override
   public Spliterator<R> spliterator() {
      return new SpliteratorMapper<>(realCollection.spliterator(), mapper);
   }

   @Override
   public Stream<R> stream() {
      return realCollection.stream().map(mapper);
   }

   @Override
   public Stream<R> parallelStream() {
      return realCollection.parallelStream().map(mapper);
   }

   @Override
   public void forEach(Consumer<? super R> action) {
      stream().forEach(action);
   }

   // Write operations are not supported!
   @Override
   public boolean add(R e) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean remove(Object o) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean addAll(Collection<? extends R> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException();
   }
}
