package org.infinispan.marshall;

import static org.infinispan.commons.marshall.MarshallUtil.NULL_VALUE;
import static org.infinispan.commons.marshall.MarshallUtil.unmarshallSize;
import static org.infinispan.commons.util.CollectionFactory.computeCapacity;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
    * Unmarshall a {@link Map} which contains user entries wrapped as {@link MarshalledEntry}.
    * <p>
    * If the marshalled map is {@link null}, then the {@link MarshallUtil.MapBuilder} is not invoked.
    *
    * @param in      {@link ObjectInput} to read.
    * @param builder {@link MarshallUtil.MapBuilder} to create the concrete {@link Map} implementation.
    * @return The populated {@link Map} created by the {@link MarshallUtil.MapBuilder} or {@code null}.
    * @throws IOException            If any of the usual Input/Output related exceptions occur.
    * @throws ClassNotFoundException If the class of a serialized object cannot be found.
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

   @FunctionalInterface
   public interface MarshalledElementWriter<E> {
      void writeTo(E element, MarshalledEntryFactory factory, ObjectOutput out) throws IOException;
   }
}
