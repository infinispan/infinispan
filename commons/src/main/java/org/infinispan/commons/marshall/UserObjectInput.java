package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;

/**
 * Extension of the {@link ObjectInput} interface, which provides a way to differentiate between "User" and "Internal"
 * objects via {@link #readUserObject()}. This class also provides utility methods for writing data
 * structures to the output.
 *
 * @author Ryan Emerson
 * @since 9.4
 */
public interface UserObjectInput extends ObjectInput {

   Object readUserObject() throws ClassNotFoundException, IOException;

   /**
    * Read a {@link Map} whose entries were serialized using the user marshaller.
    * <p>
    * If the marshalled map is {@link null}, then the {@link MapBuilder} is not invoked.
    *
    * @param builder {@link MapBuilder} to create the concrete {@link Map} implementation.
    * @return The populated {@link Map} created by the {@link MapBuilder} or {@code null}.
    * @throws IOException            If any of the usual Input/Output related exceptions occur.
    * @throws ClassNotFoundException If the class of a serialized object cannot be found.
    */
   <K, V, T extends Map<K, V>> T readUserMap(MapBuilder<K, V, T> builder) throws IOException, ClassNotFoundException;

   /**
    * Read a {@link Collection} whose elements were serialized using the user marshaller
    * <p>
    * If the marshalled map is {@link null}, then the {@link CollectionBuilder} is not invoked.
    *
    * @param builder {@link CollectionBuilder} to create the concrete {@link Collection} implementation.
    * @return The populated {@link Collection} created by the {@link CollectionBuilder} or {@code null}.
    * @throws IOException            If any of the usual Input/Output related exceptions occur.
    * @throws ClassNotFoundException If the class of a serialized object cannot be found.
    */
   <E, T extends Collection<E>> T readUserCollection(CollectionBuilder<E, T> builder) throws IOException, ClassNotFoundException;

   /**
    * Read a {@link Collection} whose elements were serialized using the user marshaller
    * <p>
    * If the marshalled map is {@link null}, then the {@link CollectionBuilder} is not invoked.
    *
    * @param builder {@link CollectionBuilder} to create the concrete {@link Collection} implementation.
    * @param reader {@link ElementReader} used to read elements from the stream.
    * @return The populated {@link Collection} created by the {@link CollectionBuilder} or {@code null}.
    * @throws IOException            If any of the usual Input/Output related exceptions occur.
    * @throws ClassNotFoundException If the class of a serialized object cannot be found.
    */
   <E, T extends Collection<E>> T readUserCollection(CollectionBuilder<E, T> builder, ElementReader<E> reader) throws IOException, ClassNotFoundException;

   <E> E[] readUserArray(ArrayBuilder<E> builder) throws IOException, ClassNotFoundException;

   /**
    * Read an array from
    *
    * @param in      {@link ObjectInput} to read.
    * @param builder {@link MarshallUtil.ArrayBuilder} to build the array.
    * @param reader {@link MarshallUtil.ElementReader} reads one element from the input.
    * @param <E>     Array type.
    * @return The populated array.
    * @throws IOException            If any of the usual Input/Output related exceptions occur.
    * @throws ClassNotFoundException If the class of a serialized object cannot be found.
    * @see #marshallArray(Object[], ObjectOutput).
    */
   <E> E[] readUserArray(ArrayBuilder<E> builder, ElementReader<E> reader) throws IOException, ClassNotFoundException;

   @FunctionalInterface
   interface ElementReader<E> {
      E readFrom(UserObjectInput input) throws ClassNotFoundException, IOException;
   }

   @FunctionalInterface
   interface MapBuilder<K, V, T extends Map<K, V>> {
      T build(int size);
   }

   @FunctionalInterface
   interface CollectionBuilder<E, T extends Collection<E>> {
      T build(int size);
   }

   @FunctionalInterface
   interface ArrayBuilder<E> {
      E[] build(int size);
   }
}
