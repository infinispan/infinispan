package org.infinispan.commons.marshall;

import net.jcip.annotations.Immutable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;

import org.infinispan.commons.io.UnsignedNumeric;

/**
 * MarshallUtil.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class MarshallUtil {

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
}
