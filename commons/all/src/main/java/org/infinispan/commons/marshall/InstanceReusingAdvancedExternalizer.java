package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An advanced externalizer that when implemented will allow for child instances that also extend this class to use object
 * instances instead of serializing a brand new object.
 * @author wburns
 * @since 7.1
 */
public abstract class InstanceReusingAdvancedExternalizer<T> extends AbstractExternalizer<T> {
   static class ReusableData {
      Map<Object, Integer> map = new HashMap<>();
      int offset;
   }
   private static ThreadLocal<ReusableData> cachedWriteObjects = new ThreadLocal<ReusableData>();
   private static ThreadLocal<List<Object>> cachedReadObjects = new ThreadLocal<List<Object>>();

   private static final int ID_NO_REPEAT                 = 0x01;
   private static final int ID_REPEAT_OBJECT_NEAR        = 0x02;
   private static final int ID_REPEAT_OBJECT_NEARISH     = 0x03;
   private static final int ID_REPEAT_OBJECT_FAR         = 0x04;

   public InstanceReusingAdvancedExternalizer() {
      this(true);
   }

   public InstanceReusingAdvancedExternalizer(boolean hasChildren) {
      this.hasChildren = hasChildren;
   }

   /**
    * This boolean controls whether or not it makes sense to actually create the context or not.  In the case of
    * classes that don't expect to have children that support this they shouldn't do the creation
    */
   private final boolean hasChildren;

   @Override
   public final void writeObject(ObjectOutput output, T object) throws IOException {
      ReusableData data = cachedWriteObjects.get();
      boolean shouldRemove;
      if (hasChildren && data == null) {
         data = new ReusableData();
         cachedWriteObjects.set(data);
         shouldRemove = true;
      } else {
         shouldRemove = false;
      }
      try {
         int id;
         if (data != null && (id = data.map.getOrDefault(object, -1)) != -1) {
            final int diff = id - data.offset;
            if (diff >= -256) {
                output.write(ID_REPEAT_OBJECT_NEAR);
                output.write(diff);
            } else if (diff >= -65536) {
                output.write(ID_REPEAT_OBJECT_NEARISH);
                output.writeShort(diff);
            } else {
                output.write(ID_REPEAT_OBJECT_FAR);
                output.writeInt(id);
            }
         } else {
            output.write(ID_NO_REPEAT);
            doWriteObject(output, object);
            // Set this before writing object in case of circular dependencies
            if (data != null) {
               data.map.put(object, data.offset++);
            }
         }
      } finally {
         if (shouldRemove) {
            cachedWriteObjects.remove();
         }
      }
   }

   public abstract void doWriteObject(ObjectOutput output, T object) throws IOException;

   @Override
   public final T readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      List<Object> data = cachedReadObjects.get();
      boolean shouldRemove;
      if (hasChildren && data == null) {
         data = new ArrayList<>();
         cachedReadObjects.set(data);
         shouldRemove = true;
      } else {
         shouldRemove = false;
      }
      try {
         int type = input.read();
         switch (type) {
         case ID_NO_REPEAT:
            T object = doReadObject(input);
            if (data != null) {
               data.add(object);
            }
            return object;
         case ID_REPEAT_OBJECT_NEAR:
            int offset = input.read() | 0xffffff00;
            return getFromCache(data, offset + data.size());
         case ID_REPEAT_OBJECT_NEARISH:
            offset = input.readShort() | 0xffff0000;
            return getFromCache(data, offset + data.size());
         case ID_REPEAT_OBJECT_FAR:
            return getFromCache(data, input.readInt());
         default:
            throw new IllegalStateException("Unexpected byte encountered: " + type);
         }
      } finally {
         if (shouldRemove) {
            cachedReadObjects.remove();
         }
      }
   }

   private T getFromCache(List<Object> data, int index) throws InvalidObjectException {
      try {
         Object obj = data.get(index);
         if (obj != null) {
            return (T) obj;
         }
     } catch (IndexOutOfBoundsException e) {
     }
     throw new InvalidObjectException("Attempt to read a backreference for " + getClass() + " with an invalid ID (absolute "
           + index + ")");
   }

   public abstract T doReadObject(ObjectInput input) throws IOException, ClassNotFoundException;
}
