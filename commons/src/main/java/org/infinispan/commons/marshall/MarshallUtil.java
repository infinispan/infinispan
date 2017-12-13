package org.infinispan.commons.marshall;

import net.jcip.annotations.Immutable;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MarshallUtil.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class MarshallUtil {

   private static final byte NULL_VALUE = -1;

   private static final Log log = LogFactory.getLog(MarshallUtil.class);

   /**
    * Marshall the {@code map} to the {@code ObjectOutput}.
    * <p>
    * {@code null} maps are supported.
    *
    * @param map {@link Map} to marshall.
    * @param out {@link ObjectOutput} to write. It must be non-null.
    * @param <K> Key type of the map.
    * @param <V> Value type of the map.
    * @param <T> Type of the {@link Map}.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   public static <K, V, T extends Map<K, V>> void marshallMap(T map, ObjectOutput out) throws IOException {
      final int mapSize = map == null ? NULL_VALUE : map.size();
      marshallInt(out, mapSize);
      if (mapSize <= 0) return;

      for (Map.Entry<K, V> me : map.entrySet()) {
         out.writeObject(me.getKey());
         out.writeObject(me.getValue());
      }
   }

   /**
    * Unmarshall the {@link Map}.
    * <p>
    * If the marshalled map is {@link null}, then the {@link MapBuilder} is not invoked.
    *
    * @param in      {@link ObjectInput} to read.
    * @param builder {@link MapBuilder} to create the concrete {@link Map} implementation.
    * @return The populated {@link Map} created by the {@link MapBuilder} or {@code null}.
    * @throws IOException            If any of the usual Input/Output related exceptions occur.
    * @throws ClassNotFoundException If the class of a serialized object cannot be found.
    * @see {@link #marshallMap(Map, ObjectOutput)}
    */
   public static <K, V, T extends Map<K, V>> T unmarshallMap(ObjectInput in, MapBuilder<K, V, T> builder) throws IOException, ClassNotFoundException {
      final int size = unmarshallInt(in);
      if (size == NULL_VALUE) {
         return null;
      }
      final T map = Objects.requireNonNull(builder, "MapBuilder must be non-null").build(size);
      for (int i = 0; i < size; i++) //noinspection unchecked
         map.put((K) in.readObject(), (V) in.readObject());
      return map;
   }

   /**
    * Marshall the {@link UUID} by sending the most and lest significant bits.
    * <p>
    * This method supports {@code null} if {@code checkNull} is set to {@link true}.
    *
    * @param uuid      {@link UUID} to marshall.
    * @param out       {@link ObjectOutput} to write.
    * @param checkNull If {@code true}, it checks if {@code uuid} is {@code null}.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   public static void marshallUUID(UUID uuid, ObjectOutput out, boolean checkNull) throws IOException {
      if (checkNull) {
         if (uuid == null) {
            out.writeBoolean(true);
            return;
         }
         out.writeBoolean(false);
      }
      out.writeLong(uuid.getMostSignificantBits());
      out.writeLong(uuid.getLeastSignificantBits());
   }

   /**
    * Unmarshall {@link UUID}.
    *
    * @param in        {@link ObjectInput} to read.
    * @param checkNull If {@code true}, it checks if the {@link UUID} marshalled was {@link null}.
    * @return {@link UUID} marshalled.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    * @see {@link #marshallUUID(UUID, ObjectOutput, boolean)}.
    */
   public static UUID unmarshallUUID(ObjectInput in, boolean checkNull) throws IOException {
      if (checkNull && in.readBoolean()) {
         return null;
      }
      return new UUID(in.readLong(), in.readLong());
   }

   /**
    * Marshall arrays.
    * <p>
    * This method supports {@code null} {@code array}.
    *
    * @param array Array to marshall.
    * @param out   {@link ObjectOutput} to write.
    * @param <E>   Array type.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   public static <E> void marshallArray(E[] array, ObjectOutput out) throws IOException {
      final int size = array == null ? NULL_VALUE : array.length;
      marshallInt(out, size);
      if (size <= 0) {
         return;
      }
      for (int i = 0; i < size; ++i) {
         out.writeObject(array[i]);
      }
   }

   /**
    * Unmarshall arrays.
    *
    * @param in      {@link ObjectInput} to read.
    * @param builder {@link ArrayBuilder} to build the array.
    * @param <E>     Array type.
    * @return The populated array.
    * @throws IOException            If any of the usual Input/Output related exceptions occur.
    * @throws ClassNotFoundException If the class of a serialized object cannot be found.
    * @see {@link #marshallArray(Object[], ObjectOutput)}.
    */
   public static <E> E[] unmarshallArray(ObjectInput in, ArrayBuilder<E> builder) throws IOException, ClassNotFoundException {
      final int size = unmarshallInt(in);
      if (size == NULL_VALUE) {
         return null;
      } else if (size == 0) {
         //noinspection unchecked
         return (E[]) Util.EMPTY_OBJECT_ARRAY;
      }
      final E[] array = Objects.requireNonNull(builder, "ArrayBuilder must be non-null").build(size);
      for (int i = 0; i < size; ++i) {
         //noinspection unchecked
         array[i] = (E) in.readObject();
      }
      return array;
   }

   /**
    * Marshall a {@link Collection}.
    * <p>
    * This method supports {@code null} {@code collection}.
    *
    * @param collection {@link Collection} to marshal.
    * @param out        {@link ObjectOutput} to write.
    * @param <E>        Collection's element type.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   public static <E> void marshallCollection(Collection<E> collection, ObjectOutput out) throws IOException {
      final int size = collection == null ? NULL_VALUE : collection.size();
      marshallInt(out, size);
      if (size <= 0) {
         return;
      }
      for (E e : collection) {
         out.writeObject(e);
      }
   }

   /**
    * Unmarshal a {@link Collection}.
    *
    * @param in      {@link ObjectInput} to read.
    * @param builder {@link CollectionBuilder} builds the concrete {@link Collection} based on size.
    * @param <E>     Collection's element type.
    * @param <T>     {@link Collection} implementation.
    * @return The concrete {@link Collection} implementation.
    * @throws IOException            If any of the usual Input/Output related exceptions occur.
    * @throws ClassNotFoundException If the class of a serialized object cannot be found.
    */
   public static <E, T extends Collection<E>> T unmarshallCollection(ObjectInput in, CollectionBuilder<E, T> builder) throws IOException, ClassNotFoundException {
      final int size = unmarshallInt(in);
      if (size == NULL_VALUE) {
         return null;
      }
      T collection = Objects.requireNonNull(builder, "CollectionBuilder must be non-null").build(size);
      for (int i = 0; i < size; ++i) {
         //noinspection unchecked
         collection.add((E) in.readObject());
      }
      return collection;
   }

   /**
    * Same as {@link #unmarshallCollection(ObjectInput, CollectionBuilder)}.
    * <p>
    * Used when the size of the {@link Collection} is not needed for it construction.
    *
    * @see {@link #unmarshallCollection(ObjectInput, CollectionBuilder)}.
    */
   public static <E, T extends Collection<E>> T unmarshallCollectionUnbounded(ObjectInput in, UnboundedCollectionBuilder<E, T> builder) throws IOException, ClassNotFoundException {
      final int size = unmarshallInt(in);
      if (size == NULL_VALUE) {
         return null;
      }
      T collection = Objects.requireNonNull(builder, "UnboundedCollectionBuilder must be non-null").build();
      for (int i = 0; i < size; ++i) {
         //noinspection unchecked
         collection.add((E) in.readObject());
      }
      return collection;
   }

   /**
    * Marshall the {@link String}.
    * <p>
    * Same behavior as {@link ObjectOutput#writeUTF(String)} but it checks for {@code null}. If the {@code string} is
    * never {@code null}, it is better to use {@link ObjectOutput#writeUTF(String)}.
    *
    * @param string {@link String} to marshall.
    * @param out    {@link ObjectOutput} to write.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   public static void marshallString(String string, ObjectOutput out) throws IOException {
      if (string == null) {
         out.writeBoolean(true);
         return;
      }
      out.writeBoolean(false);
      out.writeUTF(string);
   }

   /**
    * Unmarshall a {@link String}.
    *
    * @param in {@link ObjectInput} to read.
    * @return The {@link String} or {@code null}.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    * @see {@link #marshallString(String, ObjectOutput)}.
    */
   public static String unmarshallString(ObjectInput in) throws IOException {
      if (in.readBoolean()) {
         return null;
      }
      return in.readUTF();
   }

   /**
    * Same as {@link #marshallArray(Object[], ObjectOutput)} but specialized for byte arrays.
    *
    * @see {@link #marshallArray(Object[], ObjectOutput)}.
    */
   public static void marshallByteArray(byte[] array, ObjectOutput out) throws IOException {
      final int size = array == null ? NULL_VALUE : array.length;
      marshallInt(out, size);
      if (size <= 0) {
         return;
      }
      out.write(array);
   }

   /**
    * Same as {@link #unmarshallArray(ObjectInput, ArrayBuilder)} but specialzed for byte array.
    * <p>
    * No {@link ArrayBuilder} is necessary.
    *
    * @see {@link  #unmarshallArray(ObjectInput, ArrayBuilder)}.
    */
   public static byte[] unmarshallByteArray(ObjectInput in) throws IOException {
      final int size = unmarshallInt(in);
      if (size == NULL_VALUE) {
         return null;
      } else if (size == 0) {
         return Util.EMPTY_BYTE_ARRAY;
      }
      byte[] array = new byte[size];
      in.readFully(array);
      return array;
   }

   /**
    * A special marshall implementation for integer.
    * <p>
    * This method supports negative values but they are handles as {@link #NULL_VALUE}. It means that the real value is
    * lost and {@link #NULL_VALUE} is returned by {@link #unmarshallInt(ObjectInput)}.
    * <p>
    * The integer is marshalled in a variable length from 1 to 5 bytes. Negatives values are always marshalled in 1
    * byte.
    *
    * @param out   {@link ObjectOutput} to write.
    * @param value Integer value to marshall.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    */
   public static void marshallInt(ObjectOutput out, int value) throws IOException {
      if (value < 0) {
         out.writeByte(0x80); //meaning it is a negative value!
         return;
      }
      if ((value & ~0x3F) == 0) { // fits in 1 byte
         out.writeByte(value & 0x3F); //first bit is 0 (== positive) and second bit is zero (== not more bytes)
         return;
      }
      out.writeByte((value & 0x3F) | 0x40); //set second bit to 1 (== more bytes)
      value >>>= 6; //6 bits written so far
      //normal code for unsigned int. only the first byte is special
      while ((value & ~0x7F) != 0) {
         out.writeByte((byte) ((value & 0x7f) | 0x80));
         value >>>= 7;
      }
      out.writeByte((byte) value);
   }

   /**
    * Unmarshall an integer.
    *
    * @param in {@link ObjectInput} to read.
    * @return The integer value or {@link #NULL_VALUE} if the original value was negative.
    * @throws IOException If any of the usual Input/Output related exceptions occur.
    * @see {@link #marshallInt(ObjectOutput, int)}.
    */
   public static int unmarshallInt(ObjectInput in) throws IOException {
      byte b = in.readByte();
      if ((b & 0x80) != 0) {
         return NULL_VALUE; //negative value
      }
      int i = b & 0x3F;
      if ((b & 0x40) == 0) {
         return i;
      }
      int shift = 6;
      do {
         b = in.readByte();
         i |= (b & 0x7F) << shift;
         shift += 7;
      } while ((b & 0x80) != 0);
      return i;

   }

   public static <E extends Enum<E>> void marshallEnum(E e, ObjectOutput output) throws IOException {
      if (e == null) {
         output.writeByte(NULL_VALUE);
      } else {
         output.writeByte(e.ordinal());
      }
   }

   public static <E extends Enum<E>> E unmarshallEnum(ObjectInput input, EnumBuilder<E> builder) throws IOException {
      final byte ordinal = input.readByte();
      if (ordinal == NULL_VALUE) {
         return null;
      }
      try {
         return Objects.requireNonNull(builder).build(ordinal);
      } catch (ArrayIndexOutOfBoundsException e) {
         throw new IOException("Unknown enum.", e);
      }
   }

   /**
    * Checks whether class name is matched by the class name white list regular expressions provided.
    *
    * @param className class to verify
    * @param whitelist list of regular expressions to match class name against
    * @return true if the class matched at least one of the regular expressions,
    *         false otherwise
    */
   public static boolean isSafeClass(String className, List<String> whitelist) {
      for (String whiteRegExp : whitelist) {
         Pattern whitePattern = Pattern.compile(whiteRegExp);
         Matcher whiteMatcher = whitePattern.matcher(className);
         if (whiteMatcher.find()) {
            if (log.isTraceEnabled())
               log.tracef("Whitelist match: '%s'", className);

            return true;
         }
      }

      return false;
   }

   public interface ArrayBuilder<E> {
      E[] build(int size);
   }

   public interface CollectionBuilder<E, T extends Collection<E>> {
      T build(int size);
   }

   public interface UnboundedCollectionBuilder<E, T extends Collection<E>> {
      T build();
   }

   public interface MapBuilder<K, V, T extends Map<K, V>> {
      T build(int size);
   }

   public interface EnumBuilder<E extends Enum<E>> {
      E build(int ordinal);
   }
}
