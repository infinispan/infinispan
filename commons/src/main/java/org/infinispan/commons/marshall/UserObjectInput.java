package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Collection;
import java.util.Map;

/**
 * The interface that should be used to reader all custom user objects from the input stream.
 *
 */
public interface UserObjectInput extends ObjectInput {

   Object readUserObject() throws ClassNotFoundException, IOException;

   /**
    * Unmarshall a {@link Map} which contains user objects.
    * <p>
    * If the marshalled map is {@link null}, then the {@link MapBuilder} is not invoked.
    *
    * @param builder {@link MapBuilder} to create the concrete {@link Map} implementation.
    * @return The populated {@link Map} created by the {@link MapBuilder} or {@code null}.
    * @throws IOException            If any of the usual Input/Output related exceptions occur.
    * @throws ClassNotFoundException If the class of a serialized object cannot be found.
    */
   <K, V, T extends Map<K, V>> T readUserMap(MapBuilder<K, V, T> builder) throws IOException, ClassNotFoundException;

   <E, T extends Collection<E>> T readUserCollection(CollectionBuilder<E, T> builder) throws IOException, ClassNotFoundException;

   <E, T extends Collection<E>> T readUserCollection(CollectionBuilder<E, T> builder, ElementReader<E> reader) throws IOException, ClassNotFoundException;

   <E> E[] readUserArray(MarshallUtil.ArrayBuilder<E> builder) throws IOException, ClassNotFoundException;

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
}
