package org.infinispan.commons.marshall;

import net.jcip.annotations.Immutable;
import org.infinispan.commons.io.UnsignedNumeric;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * MarshallUtil.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class MarshallUtil {

   private static final byte NULL_VALUE = -1;

   public static void marshallCollection(Collection<?> c, ObjectOutput out) throws IOException {
      UnsignedNumeric.writeUnsignedInt(out, c.size());
      for (Object o : c) {
         out.writeObject(o);
      }
   }

   public static void marshallMap(Map<?, ?> map, ObjectOutput out) throws IOException {
      int mapSize = map.size();
      UnsignedNumeric.writeUnsignedInt(out, mapSize);
      if (mapSize == 0) return;

      for (Map.Entry<?, ?> me : map.entrySet()) {
         out.writeObject(me.getKey());
         out.writeObject(me.getValue());
      }
   }

   public static void unmarshallMap(Map<Object, Object> map, ObjectInput in) throws IOException, ClassNotFoundException {
      int size = UnsignedNumeric.readUnsignedInt(in);
      for (int i = 0; i < size; i++) map.put(in.readObject(), in.readObject());
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

   public interface EnumBuilder<E extends Enum<E>> {
      E build(int ordinal);
   }
}
