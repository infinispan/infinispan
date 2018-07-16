package org.infinispan.marshall.core;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.marshall.MarshalledEntryUtil;
import org.infinispan.metadata.Metadata;

/**
 * The interface that should be used to write all user objects to the output stream provided by the implementation.
 *
 * @author remerson
 * @since 4.0
 */
public interface UserObjectOutput<K,V> {

   void writeGroupName(K group, MarshalledEntryFactory factory, ObjectOutput out) throws IOException;

   void writeKey(K key, MarshalledEntryFactory factory, ObjectOutput out) throws IOException;

   void writeValue(V value, MarshalledEntryFactory factory, ObjectOutput out) throws IOException;

   void writeKeyValue(K key, V value, MarshalledEntryFactory factory, ObjectOutput out) throws IOException;

   void writeMetadata(Metadata metadata, MarshalledEntryFactory factory, ObjectOutput out) throws IOException;

   void write(K key, V value, Metadata metadata) throws IOException;

   K readGroupName(ObjectInput in) throws ClassNotFoundException, IOException;

   K readKey(ObjectInput in) throws ClassNotFoundException, IOException;

   V readValue(ObjectInput in) throws ClassNotFoundException, IOException;

   Metadata readMetadata(ObjectInput in) throws ClassNotFoundException, IOException;

   MarshalledEntryImpl<K, V> read(ObjectInput in) throws ClassNotFoundException, IOException;

   /**
    * Marshall the {@code map} to the {@code ObjectOutput} using the user marshaller to serialize the key/values.
    * <p>
    * {@code null} maps are supported.
    *
    * @param map     {@link Map} to marshall.
    * @param out     {@link ObjectOutput} to write. It must be non-null.
    * @param factory {@link MarshalledEntryFactory} used to create the {@link org.infinispan.marshall.core.MarshalledEntry}
    *                wrapper.
    * @param <T>     Type of the {@link Map}.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   <T extends Map<K, V>> void marshallMap(T map, MarshalledEntryFactory factory, ObjectOutput out) throws IOException;

   /**
    * Unmarshall a {@link Map} which contains user entries wrapped as {@link MarshalledEntry}.
    * <p>
    * If the marshalled map is {@link null}, then the {@link MarshallUtil.MapBuilder} is not invoked.
    *
    * @param in      {@link ObjectInput} to read.
    * @param builder {@link MarshallUtil.MapBuilder} to create the concrete {@link Map} implementation.
    * @return The populated {@link Map} created by the {@link MarshallUtil.MapBuilder} or {@code null}.
    * @throws IOException            If any of the usual Input/Output related exceptions occur.
    * @throws ClassNotFoundException If the class of a serialized object cannot be found.
    * @see #marshallMap(Map, MarshalledEntryFactory marshalledEntryFactory, ObjectOutput)
    */
   <T extends Map<K, V>> T unmarshallMap(ObjectInput in, MarshallUtil.MapBuilder<K, V, T> builder) throws IOException, ClassNotFoundException;

   /**
    * Marshall a {@link Collection}.
    * <p>
    * This method supports {@code null} {@code collection}.
    *
    * @param collection {@link Collection} to marshal.
    * @param factory    {@link MarshalledEntryFactory} to write.
    * @param out        {@link ObjectOutput} to write.
    * @param writer     {@link MarshalledEntryUtil.MarshalledElementWriter} that writes single element to the output.
    * @param <E>        Collection's element type.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   <E> void marshallCollection(Collection<E> collection, MarshalledEntryFactory factory, ObjectOutput out, MarshalledEntryUtil.MarshalledElementWriter<E> writer) throws IOException;

   /**
    * Marshall arrays.
    * <p>
    * This method supports {@code null} {@code array}.
    *
    * @param array   Array to marshall.
    * @param factory {@link MarshalledEntryFactory} to write.
    * @param out     {@link ObjectOutput} to write.
    * @param writer  {@link MarshalledEntryUtil.MarshalledElementWriter} that writes single element to the output.
    * @param <E>     Array type.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   <E> void marshallArray(E[] array, MarshalledEntryFactory factory, ObjectOutput out, MarshalledEntryUtil.MarshalledElementWriter<E> writer) throws IOException;

   @FunctionalInterface
   interface MarshalledElementWriter<E> {
      void writeTo(E element, MarshalledEntryFactory factory, ObjectOutput out) throws IOException;
   }
}
