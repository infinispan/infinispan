package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;

/**
 * Extension of the {@link ObjectOutput} interface, which provides a way to differentiate between "User" and "Internal"
 * objects via {@link #writeUserObject(Object)}. This class also provides utility methods for reading data
 * structures from the input stream.
 *
 * @author Ryan Emerson
 * @since 9.4
 */
public interface UserObjectOutput extends ObjectOutput {

   void writeKey(Object key) throws IOException;

   void writeValue(Object value) throws IOException;

   void writeKeyValue(Object key, Object value) throws IOException;

   void writeEntry(Object key, Object value, Object metadata) throws IOException;

   /**
    * Writes the provided object to the stream using using the configured user marshaller to generate the resulting bytes.
    *
    * @param object the user object to write to the stream
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   void writeUserObject(Object object) throws IOException;

   /**
    * Write the {@code map} to the stream using {@link #writeUserObject(Object)} to write entries.
    * <p>
    * {@code null} maps are supported.
    *
    * @param <K>     Key type of the map.
    * @param <V>     Value type of the map.
    * @param <T>     Type of the {@link Map}.
    * @param map     {@link Map} to marshall.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   <K, V, T extends Map<K, V>> void writeUserMap(T map) throws IOException;

   /**
    * Write the {@code map} to the stream using the provided {@link ElementWriter}.
    * <p>
    * {@code null} maps are supported.
    *
    * @param <K>     Key type of the map.
    * @param <V>     Value type of the map.
    * @param <T>     Type of the {@link Map}.
    * @param map     {@link Map} to marshall.
    * @param keyWriter {@link ElementWriter} which determines how an entries key is written to the output.
    * @param valueWriter {@link ElementWriter} which determines how an entries value is written to the output.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   <K, V, T extends Map<K, V>> void writeUserMap(T map, ElementWriter<K> keyWriter, ElementWriter<V> valueWriter) throws IOException;

   /**
    * Write the {@link Collection} to the stream using using {@link #writeUserObject(Object)} to write elements.
    * <p>
    * This method supports {@code null} {@code collection}.
    *
    * @param <E>        Collection's element type.
    * @param collection {@link Collection} to marshal.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   <E> void writeUserCollection(Collection<E> collection) throws IOException;

   /**
    * Write the {@link Collection} to the stream using the provided {@link ElementWriter}.
    * <p>
    * This method supports {@code null} {@code collection}.
    *
    * @param <E>        Collection's element type.
    * @param collection {@link Collection} to marshall.
    * @param writer {@link ElementWriter} which determines how the collections elements are written to the output.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   <E> void writeUserCollection(Collection<E> collection, ElementWriter<E> writer) throws IOException;

   /**
    * Write arrays to the stream using {@link #writeUserObject(Object)} to serialize elements.
    * <p>
    * This method supports {@code null} {@code array}.
    *
    * @param <E>     Array type.
    * @param array   Array to marshall.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   <E> void writeUserArray(E[] array) throws IOException;

   /**
    * Write arrays to the stream using the provided {@link ElementWriter}.
    * <p>
    * This method supports {@code null} {@code array}.
    *
    * @param <E>     Array type.
    * @param array   Array to marshall.
    * @param writer {@link ElementWriter} which determines how the arrays elements are written to the output.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   <E> void writeUserArray(E[] array, ElementWriter<E> writer) throws IOException;

   @FunctionalInterface
   interface ElementWriter<E> {
      void writeTo(UserObjectOutput out, E element) throws IOException;
   }
}
