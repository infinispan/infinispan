package org.infinispan.marshall;

import static org.infinispan.commons.marshall.MarshallUtil.NULL_VALUE;
import static org.infinispan.commons.marshall.MarshallUtil.marshallSize;
import static org.infinispan.commons.marshall.MarshallUtil.unmarshallSize;
import static org.infinispan.commons.util.CollectionFactory.computeCapacity;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.metadata.Metadata;

/**
 * Util class for writing user key/values to the Object input/outputs used by the internal marshaller.
 */
public class MarshalledEntryUtil {

   // Just write as a MarshalledEntry
   public static <K> void writeGroupName(K group) throws IOException {
      writeKey(group);
   }


   public static <K> void writeKey(K key) throws IOException {
      write(key, null, null);
   }

   public static <V> void writeValue(V value) throws IOException {
      write(null, value, null);
   }

   public static <K, V> void writeKeyValue(K key, V value) throws IOException {
      write(key, value, null);
   }

   public static void writeMetadata(Metadata metadata) throws IOException {
      write(null, null, metadata);
   }

   public static <K, V> void write(K key, V value, Metadata metadata) throws IOException {
      out.writeObject(factory.newMarshalledEntry(key, value, metadata));
   }

   public static <K> K readGroupName(ObjectInput in) throws ClassNotFoundException, IOException {
      return readKey(in);
   }

   public static <K> K readKey(ObjectInput in) throws ClassNotFoundException, IOException {
      MarshalledEntry<K, ?> entry = read(in);
      return entry.getKey();
   }

   public static <V> V readValue(ObjectInput in) throws ClassNotFoundException, IOException {
      MarshalledEntry<?, V> entry = read(in);
      return entry.getValue();
   }

   public static Metadata readMetadata(ObjectInput in) throws ClassNotFoundException, IOException {
      return read(in).metadata();
   }

   public static <K, V> MarshalledEntryImpl<K, V> read(ObjectInput in) throws ClassNotFoundException, IOException {
      //noinspection unchecked
      return (MarshalledEntryImpl<K, V>) in.readObject();
   }

   /**
    * Marshall the {@code map} to the {@code ObjectOutput} using the user marshaller to serialize the key/values.
    * <p>
    * {@code null} maps are supported.
    *
    * @param <K>     Key type of the map.
    * @param <V>     Value type of the map.
    * @param <T>     Type of the {@link Map}.
    * @param map     {@link Map} to marshall.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   public static <K, V, T extends Map<K, V>> void marshallMap(T map) throws IOException {
      final int mapSize = map == null ? NULL_VALUE : map.size();
      marshallSize(out, mapSize);
      if (mapSize <= 0) return;

      for (Map.Entry<K, V> entry : map.entrySet()) {
         MarshalledEntry me = factory.newMarshalledEntry(entry.getKey(), entry.getValue(), null);
         out.writeObject(me);
      }
   }

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
    * @see #marshallMap(Map)
    */
   public static <K, V, T extends Map<K, V>> T unmarshallMap(ObjectInput in, MarshallUtil.MapBuilder<K, V, T> builder) throws IOException, ClassNotFoundException {
      final int size = unmarshallSize(in);
      if (size == NULL_VALUE) {
         return null;
      }
      final T map = Objects.requireNonNull(builder, "MapBuilder must be non-null").build(computeCapacity(size));
      for (int i = 0; i < size; i++) {
         //noinspection unchecked
         MarshalledEntryImpl<K, V> me = read(in);
         map.put(me.getKey(), me.getValue());
      }
      return map;
   }

   /**
    * Marshall a {@link Collection}.
    * <p>
    * This method supports {@code null} {@code collection}.
    *
    * @param <E>        Collection's element type.
    * @param collection {@link Collection} to marshal.
    * @param writer     {@link MarshalledElementWriter} that writes single element to the output.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   public static <E> void marshallCollection(Collection<E> collection, MarshalledElementWriter<E> writer) throws IOException {
      final int size = collection == null ? NULL_VALUE : collection.size();
      marshallSize(out, size);
      if (size <= 0) {
         return;
      }
      for (E e : collection) {
         writer.writeTo(e, factory, out);
      }
   }

   /**
    * Marshall arrays.
    * <p>
    * This method supports {@code null} {@code array}.
    *
    * @param <E>     Array type.
    * @param array   Array to marshall.
    * @param writer  {@link MarshalledElementWriter} that writes single element to the output.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   public static <E> void marshallArray(E[] array, MarshalledElementWriter<E> writer) throws IOException {
      final int size = array == null ? NULL_VALUE : array.length;
      marshallSize(out, size);
      if (size <= 0) {
         return;
      }
      for (int i = 0; i < size; ++i) {
         writer.writeTo(array[i], factory, out);
      }
   }

   @FunctionalInterface
   public interface MarshalledElementWriter<E> {
      void writeTo(E element, MarshalledEntryFactory factory, ObjectOutput out) throws IOException;
   }
}
