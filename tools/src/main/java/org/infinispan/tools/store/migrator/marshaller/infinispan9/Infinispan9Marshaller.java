package org.infinispan.tools.store.migrator.marshaller.infinispan9;

import java.io.IOException;
import java.io.ObjectInput;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.CacheException;
import org.infinispan.tools.store.migrator.marshaller.common.AdvancedExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.Externalizer;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.tools.store.migrator.marshaller.common.AbstractUnsupportedStreamingMarshaller;

/**
 * Legacy marshaller for reading from Infinispan 9.x stores.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class Infinispan9Marshaller extends AbstractUnsupportedStreamingMarshaller {

   private static final int ID_NULL = 0x00;
   private static final int ID_PRIMITIVE = 0x01;
   private static final int ID_INTERNAL = 0x02;
   private static final int ID_EXTERNAL = 0x03;
   private static final int ID_ANNOTATED = 0x04;
   private static final int ID_UNKNOWN = 0x05;
   private static final int ID_ARRAY = 0x06;
   private static final int ID_CLASS = 0x07;

   private static final int TYPE_MASK = 0x07;
   private static final int ARRAY_SIZE_MASK = 0xC0;
   private static final int FLAG_SINGLE_TYPE = 0x08;
   private static final int FLAG_COMPONENT_TYPE_MATCH = 0x10;
   private static final int FLAG_ALL_NULL = 0x20;
   private static final int FLAG_ARRAY_EMPTY = 0x00;
   private static final int FLAG_ARRAY_SMALL = 0x40;
   private static final int FLAG_ARRAY_MEDIUM = 0x80;
   private static final int FLAG_ARRAY_LARGE = 0xC0;

   private final ExternalJbossMarshaller external;
   private final Map<Integer, AdvancedExternalizer> exts;

   public Infinispan9Marshaller(Map<Integer, AdvancedExternalizer> userExts) {
      this.exts = ExternalizerTable.getInternalExternalizers(this);
      this.exts.putAll(userExts);
      this.external = new ExternalJbossMarshaller(this);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      return readNullableObject(new BytesObjectInput(buf, offset, this));
   }

   <T> Externalizer<T> findExternalizerIn(ObjectInput in) throws IOException {
      int type = in.readUnsignedByte();
      switch (type) {
         case ID_INTERNAL:
            return exts.get(in.readUnsignedByte());
         case ID_EXTERNAL:
            return exts.get(in.readInt());
         default:
            return null;
      }
   }

   Object readNullableObject(BytesObjectInput in) throws IOException, ClassNotFoundException {
      int type = in.readUnsignedByte();
      if (type == ID_NULL)
         return null;

      switch (type) {
         case ID_PRIMITIVE:
            return Primitives.readPrimitive(in);
         case ID_INTERNAL:
            return exts.get(in.readUnsignedByte()).readObject(in);
         case ID_EXTERNAL:
            return exts.get(in.readInt()).readObject(in);
         case ID_ANNOTATED:
            return readAnnotated(in);
         case ID_UNKNOWN:
            return external.objectFromObjectStream(in);
         case ID_ARRAY:
            return readArray(in);
         default:
            throw new IOException("Unknown type: " + type);
      }
   }

   private Object readAnnotated(BytesObjectInput in) throws IOException, ClassNotFoundException {
      Class<? extends Externalizer> clazz = (Class<? extends Externalizer>) in.readObject();
      try {
         Externalizer ext = clazz.newInstance();
         return ext.readObject(in);
      } catch (Exception e) {
         throw new CacheException("Error instantiating class: " + clazz, e);
      }
   }

   private Object readArray(BytesObjectInput in) throws IOException, ClassNotFoundException {
      int flags = in.readByte();
      int type = flags & TYPE_MASK;
      AdvancedExternalizer<?> componentExt = null;
      Class<?> extClazz = null;
      Class<?> componentType;
      switch (type) {
         case ID_NULL:
         case ID_PRIMITIVE:
         case ID_ARRAY:
            throw new IOException("Unexpected component type: " + type);
         case ID_INTERNAL:
            componentExt = exts.get((int) in.readByte());
            componentType = getOrReadClass(in, componentExt);
            break;
         case ID_EXTERNAL:
            componentExt = exts.get(in.readInt());
            componentType = getOrReadClass(in, componentExt);
            break;
         case ID_ANNOTATED:
            extClazz = (Class<?>) in.readObject();
            // intentional no break
         case ID_UNKNOWN:
            componentType = (Class<?>) in.readObject();
            break;
         case ID_CLASS:
            componentType = getClass(in.readByte());
            break;
         default:
            throw new IOException("Unknown component type: " + type);
      }
      int length;
      int maskedSize = flags & ARRAY_SIZE_MASK;
      switch (maskedSize) {
         case FLAG_ARRAY_EMPTY:
            length = 0;
            break;
         case FLAG_ARRAY_SMALL:
            length = in.readUnsignedByte() + Primitives.SMALL_ARRAY_MIN;
            break;
         case FLAG_ARRAY_MEDIUM:
            length = in.readUnsignedShort() + Primitives.MEDIUM_ARRAY_MIN;
            break;
         case FLAG_ARRAY_LARGE:
            length = in.readInt();
            break;
         default:
            throw new IOException("Unknown array size: " + maskedSize);
      }
      Object array = Array.newInstance(componentType, length);
      if ((flags & FLAG_ALL_NULL) != 0) {
         return array;
      }

      boolean singleType = (flags & FLAG_SINGLE_TYPE) != 0;
      boolean componentTypeMatch = (flags & FLAG_COMPONENT_TYPE_MATCH) != 0;
      // If component type match is set, this must be a single type
      assert !componentTypeMatch || singleType;
      if (singleType) {
         Externalizer<?> ext;
         if (componentTypeMatch) {
            ext = getArrayElementExternalizer(type, componentExt, extClazz);
         } else {
            type = in.readByte();
            ext = readExternalizer(in, type);
         }
         if (ext != null) {
            for (int i = 0; i < length; ++i) {
               Array.set(array, i, ext.readObject(in));
            }
         } else {
            switch (type) {
               case ID_UNKNOWN:
                  for (int i = 0; i < length; ++i) {
                     Array.set(array, i, external.objectFromObjectStream(in));
                  }
                  return array;
               case ID_PRIMITIVE:
                  int primitiveId = in.readByte();
                  for (int i = 0; i < length; ++i) {
                     Array.set(array, i, Primitives.readRawPrimitive(in, primitiveId));
                  }
                  return array;
               default:
                  throw new IllegalStateException();
            }
         }
      } else {
         for (int i = 0; i < length; ++i) {
            Array.set(array, i, readNullableObject(in));
         }
      }
      return array;
   }

   private Class<?> getClass(int id) throws IOException {
      switch (id) {
         case 0: return Object.class;
         case 1: return String.class;
         case 2: return List.class;
         case 3: return Map.Entry.class;
         case 16: return InternalCacheValue.class;
         default: throw new IOException("Unknown class id " + id);
      }
   }

   private Externalizer<?> getArrayElementExternalizer(int type, AdvancedExternalizer<?> componentExt, Class<?> extClazz) throws IOException {
      switch (type) {
         case ID_INTERNAL:
         case ID_EXTERNAL:
            return componentExt;
         case ID_ANNOTATED:
            try {
               return (Externalizer<?>) extClazz.newInstance();
            } catch (Exception e) {
               throw new CacheException("Error instantiating class: " + extClazz, e);
            }
         case ID_UNKNOWN:
            return null;
         default:
            throw new IOException("Unexpected component type: " + type);
      }
   }

   private Externalizer<?> readExternalizer(BytesObjectInput in, int type) throws ClassNotFoundException, IOException {
      Class<?> extClazz;
      switch (type) {
         case ID_INTERNAL:
            return exts.get(0xFF & in.readByte());
         case ID_EXTERNAL:
            return exts.get(in.readInt());
         case ID_ANNOTATED:
            extClazz = (Class<?>) in.readObject();
            try {
               return (Externalizer<?>) extClazz.newInstance();
            } catch (Exception e) {
               throw new CacheException("Error instantiating class: " + extClazz, e);
            }
         case ID_UNKNOWN:
         case ID_PRIMITIVE:
            return null;
         default:
            throw new IOException("Unexpected component type: " + type);
      }
   }

   private Class<?> getOrReadClass(BytesObjectInput in, AdvancedExternalizer<?> componentExt) throws ClassNotFoundException, IOException {
      if (componentExt.getTypeClasses().size() == 1) {
         return componentExt.getTypeClasses().iterator().next();
      } else {
         return (Class<?>) in.readObject();
      }
   }
}
