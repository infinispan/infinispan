package org.infinispan.marshall.core;

import static org.infinispan.commons.marshall.MarshallUtil.NULL_VALUE;
import static org.infinispan.commons.marshall.MarshallUtil.marshallSize;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.infinispan.commons.marshall.UserObjectOutput;

/**
 * Abstract class which provides implementations for all methods introduced in {@link UserObjectOutput} except {@link
 * UserObjectOutput#writeUserObject(Object)} which must be provided by the concrete class.
 *
 * @author remerson
 * @since 9.4
 */
abstract class AbstractUserObjectOutput implements UserObjectOutput {

   @Override
   public void writeUserObjects(Object... objects) throws IOException {
      for (Object object : objects)
         writeUserObject(object);
   }

   @Override
   public <K, V, T extends Map<K, V>> void writeUserMap(T map) throws IOException {
      writeUserMap(map, UserObjectOutput::writeUserObject, UserObjectOutput::writeUserObject);
   }

   @Override
   public <K, V, T extends Map<K, V>> void writeUserMap(T map, ElementWriter<K> keyWriter, ElementWriter<V> valueWriter) throws IOException {
      final int mapSize = map == null ? NULL_VALUE : map.size();
      marshallSize(this, mapSize);
      if (mapSize <= 0) return;

      for (Map.Entry<K, V> me : map.entrySet()) {
         keyWriter.writeTo(this, me.getKey());
         valueWriter.writeTo(this, me.getValue());
      }
   }

   @Override
   public <E> void writeUserCollection(Collection<E> collection) throws IOException {
      writeUserCollection(collection, UserObjectOutput::writeUserObject);
   }

   public <E> void writeUserCollection(Collection<E> collection, ElementWriter<E> writer) throws IOException {
      final int size = collection == null ? NULL_VALUE : collection.size();
      marshallSize(this, size);
      if (size <= 0) {
         return;
      }
      for (E e : collection) {
         writer.writeTo(this, e);
      }
   }

   @Override
   public <E> void writeUserArray(E[] array) throws IOException {
      writeUserArray(array, UserObjectOutput::writeUserObject);
   }

   @Override
   public <E> void writeUserArray(E[] array, ElementWriter<E> writer) throws IOException {
      final int size = array == null ? NULL_VALUE : array.length;
      marshallSize(this, size);
      if (size <= 0) {
         return;
      }
      for (int i = 0; i < size; ++i) {
         writeUserObject(array[i]);
      }
   }
}
