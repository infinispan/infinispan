package org.infinispan.marshall.core;

import java.io.IOException;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;

import org.infinispan.marshall.MarshalledEntryUtil;
import org.infinispan.metadata.Metadata;

/**
 * The interface that should be used to write all user objects to the output stream provided by the implementation.
 *
 * @author remerson
 * @since 4.0
 */
public interface UserAwareObjectOutput extends ObjectOutput {

   void writeGroupName(Object object) throws IOException;

   void writeKey(Object keyObject) throws IOException;

   void writeValue(Object valueObject) throws IOException;

   void writeKeyValue(Object key, Object valueObject) throws IOException;

   void writeMetadata(Metadata metadataObject) throws IOException;

   void writeEntry(Object key, Object value, Metadata metadata) throws IOException;

   /**
    * Marshall the {@code map} to the delegate {@code ObjectOutput} using the user marshaller to serialize objects.
    * <p>
    * {@code null} maps are supported.
    *
    * @param <K>     Key type of the map.
    * @param <V>     Value type of the map.
    * @param <T>     Type of the {@link Map}.
    * @param map     {@link Map} to marshall.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   <K, V, T extends Map<K, V>> void marshallMap(T map) throws IOException;

   /**
    * Marshall a {@link Collection}.
    * <p>
    * This method supports {@code null} {@code collection}.
    *
    * @param <E>        Collection's element type.
    * @param collection {@link Collection} to marshal.
    * @param writer     {@link MarshalledEntryUtil.MarshalledElementWriter} that writes single element to the output.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   <E> void marshallCollection(Collection<E> collection, UserObjectWriter<E> writer) throws IOException;

   /**
    * Marshall arrays.
    * <p>
    * This method supports {@code null} {@code array}.
    *
    * @param <E>     Array type.
    * @param array   Array to marshall.
    * @param writer  {@link MarshalledEntryUtil.MarshalledElementWriter} that writes single element to the output.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   <E> void marshallArray(E[] array, UserObjectWriter<E> writer) throws IOException;

   @FunctionalInterface
   interface UserObjectWriter<E> {
      void writeTo(UserAwareObjectOutput out, E element) throws IOException;
   }
}
