package org.infinispan.marshall.core;

import static org.infinispan.commons.marshall.MarshallUtil.NULL_VALUE;
import static org.infinispan.commons.marshall.MarshallUtil.unmarshallSize;
import static org.infinispan.commons.util.CollectionFactory.computeCapacity;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import org.infinispan.commons.marshall.UserObjectInput;

/**
 * Abstract class which provides implementations for all methods introduced in {@link UserObjectInput} except {@link UserObjectInput#readUserObject()}
 * which must be provided by the concrete class.
 *
 * @author remerson
 * @since 9.4
 */
abstract class AbstractUserObjectInput implements UserObjectInput {

   @Override
   public <K, V, T extends Map<K, V>> T readUserMap(MapBuilder<K, V, T> builder) throws IOException, ClassNotFoundException {
      final int size = unmarshallSize(this);
      if (size == NULL_VALUE) {
         return null;
      }
      final T map = Objects.requireNonNull(builder, "MapBuilder must be non-null").build(computeCapacity(size));
      for (int i = 0; i < size; i++) {
         //noinspection unchecked
         map.put((K) readUserObject(), (V) readUserObject());
      }
      return map;
   }

   @Override
   public <E, T extends Collection<E>> T readUserCollection(CollectionBuilder<E, T> builder) throws IOException, ClassNotFoundException {
      return readUserCollection(builder, in -> (E) in.readUserObject());
   }

   @Override
   public <E, T extends Collection<E>> T readUserCollection(CollectionBuilder<E, T> builder, ElementReader<E> reader) throws IOException, ClassNotFoundException {
      final int size = unmarshallSize(this);
      if (size == NULL_VALUE) {
         return null;
      }
      T collection = Objects.requireNonNull(builder, "CollectionBuilder must be non-null").build(size);
      for (int i = 0; i < size; ++i) {
         //noinspection unchecked
         collection.add(reader.readFrom(this));
      }
      return collection;
   }

   @Override
   public <E> E[] readUserArray(ArrayBuilder<E> builder) throws IOException, ClassNotFoundException {
      return readUserArray(builder, in -> (E) in.readUserObject());
   }

   @Override
   public <E> E[] readUserArray(ArrayBuilder<E> builder, ElementReader<E> reader) throws IOException, ClassNotFoundException {
      final int size = unmarshallSize(this);
      if (size == NULL_VALUE) {
         return null;
      }
      final E[] array = Objects.requireNonNull(builder, "ArrayBuilder must be non-null").build(size);
      for (int i = 0; i < size; ++i) {
         //noinspection unchecked
         array[i] = reader.readFrom(this);
      }
      return array;
   }

}
