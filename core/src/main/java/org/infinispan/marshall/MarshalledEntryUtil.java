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

   public static <K> void writeKey(K key, MarshalledEntryFactory factory, ObjectOutput out) throws IOException {
      write(key, null, null, factory, out);
   }

   public static <V> void writeValue(V value, MarshalledEntryFactory factory, ObjectOutput out) throws IOException {
      write(null, value, null, factory, out);
   }

   public static <K, V> void writeKeyValue(K key, V value, MarshalledEntryFactory factory, ObjectOutput out) throws IOException {
      write(key, value, null, factory, out);
   }

   public static void writeMetadata(Metadata metadata, MarshalledEntryFactory factory, ObjectOutput out) throws IOException {
      write(null, null, metadata, factory, out);
   }

   public static <K, V> void write(K key, V value, Metadata metadata, MarshalledEntryFactory factory, ObjectOutput out) throws IOException {
      try {
         out.writeObject(factory.newMarshalledEntry(key, value, metadata));
      } catch (NullPointerException e) {
         throw e;
      }
   }

   public static <K, V> K readKey(ObjectInput in) throws ClassNotFoundException, IOException {
      MarshalledEntry<K, V> entry = read(in);
      return entry.getKey();
   }

   public static <K, V> V readValue(ObjectInput in) throws ClassNotFoundException, IOException {
      MarshalledEntry<K, V> entry = read(in);
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
    * @param map     {@link Map} to marshall.
    * @param out     {@link ObjectOutput} to write. It must be non-null.
    * @param factory {@link MarshalledEntryFactory} used to create the {@link org.infinispan.marshall.core.MarshalledEntry}
    *                wrapper.
    * @param <K>     Key type of the map.
    * @param <V>     Value type of the map.
    * @param <T>     Type of the {@link Map}.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   public static <K, V, T extends Map<K, V>> void marshallMap(T map, MarshalledEntryFactory factory, ObjectOutput out) throws IOException {
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
    * @see #marshallMap(Map, MarshalledEntryFactory marshalledEntryFactory, ObjectOutput)
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
    * @param collection {@link Collection} to marshal.
    * @param factory    {@link MarshalledEntryFactory} to write.
    * @param out        {@link ObjectOutput} to write.
    * @param writer     {@link MarshalledElementWriter} that writes single element to the output.
    * @param <E>        Collection's element type.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   public static <E> void marshallCollection(Collection<E> collection, MarshalledEntryFactory factory, ObjectOutput out, MarshalledElementWriter<E> writer) throws IOException {
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
    * @param array   Array to marshall.
    * @param factory {@link MarshalledEntryFactory} to write.
    * @param out     {@link ObjectOutput} to write.
    * @param writer  {@link MarshalledElementWriter} that writes single element to the output.
    * @param <E>     Array type.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   public static <E> void marshallArray(E[] array, MarshalledEntryFactory factory, ObjectOutput out, MarshalledElementWriter<E> writer) throws IOException {
      final int size = array == null ? NULL_VALUE : array.length;
      marshallSize(out, size);
      if (size <= 0) {
         return;
      }
      for (int i = 0; i < size; ++i) {
         writer.writeTo(array[i], factory, out);
      }
   }

   /**
    * Unmarshall arrays.
    *
    * @param in      {@link ObjectInput} to read.
    * @param builder {@link MarshallUtil.ArrayBuilder} to build the array.
    * @param <E>     Array type.
    * @return The populated array.
    * @throws IOException            If any of the usual Input/Output related exceptions occur.
    * @throws ClassNotFoundException If the class of a serialized object cannot be found.
    * @see #marshallArray(Object[], ObjectOutput).
    */
   public static <E> E[] unmarshallArray(ObjectInput in, MarshallUtil.ArrayBuilder<E> builder) throws IOException, ClassNotFoundException {
      final int size = unmarshallSize(in);
      if (size == NULL_VALUE) {
         return null;
      }
      final E[] array = Objects.requireNonNull(builder, "ArrayBuilder must be non-null").build(size);
      for (int i = 0; i < size; ++i) {
         //noinspection unchecked
         array[i] = (E) in.readObject();
      }
      return array;
   }

   @FunctionalInterface
   public interface MarshalledElementWriter<E> {
      void writeTo(E element, MarshalledEntryFactory factory, ObjectOutput out) throws IOException;
   }
}
