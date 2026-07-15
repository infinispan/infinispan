package org.infinispan.query.core.impl;

import static org.infinispan.query.core.impl.Log.CONTAINER;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.ProtobufFieldUpdater;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.WireType;
import org.infinispan.protostream.impl.RandomAccessOutputStreamImpl;
import org.infinispan.protostream.impl.TagReaderImpl;
import org.infinispan.protostream.impl.TagWriterImpl;
import org.infinispan.query.objectfilter.impl.syntax.parser.IckleParsingResult;

/**
 * Shared logic for applying Ickle UPDATE statement operations to cache entries.
 * <p>
 * Handles both protobuf-encoded entries (byte[] / WrappedByteArray, used by Hot Rod / remote caches)
 * and Java object entries (used by embedded caches) transparently.
 * For Java objects, the value is serialized to protobuf, updated via {@link ProtobufFieldUpdater},
 * then deserialized back.
 * <p>
 * Used by {@link EmbeddedQuery}, {@link HybridQuery}, and their subclasses.
 *
 * @since 16.3
 */
public final class UpdateQueryHelper {

   private UpdateQueryHelper() {
   }

   public static List<ProtobufFieldUpdater.UpdateOperation> toProtobufOps(List<IckleParsingResult.UpdateOperation> updateOperations) {
      List<ProtobufFieldUpdater.UpdateOperation> ops = new ArrayList<>();
      for (IckleParsingResult.UpdateOperation uo : updateOperations) {
         ProtobufFieldUpdater.OperationType opType = switch (uo.getType()) {
            case SET -> ProtobufFieldUpdater.OperationType.SET;
            case ADD -> ProtobufFieldUpdater.OperationType.ADD;
            case REMOVE -> ProtobufFieldUpdater.OperationType.REMOVE;
         };
         ops.add(new ProtobufFieldUpdater.UpdateOperation(opType, uo.getPropertyPath(), uo.getValues()));
      }
      return ops;
   }

   public static boolean applyUpdate(AdvancedCache<Object, Object> cache, Object key,
                               ImmutableSerializationContext serCtx,
                               List<ProtobufFieldUpdater.UpdateOperation> ops) throws IOException {
      AdvancedCache<Object, Object> storageCache = cache.withStorageMediaType();
      Object existingValue = storageCache.get(key);
      if (existingValue == null) return false;

      if (existingValue instanceof byte[] || existingValue instanceof WrappedByteArray) {
         return applyProtobufUpdate(storageCache, key, existingValue, serCtx, ops);
      }

      // Java object stored in embedded mode — try protobuf roundtrip first, fall back to reflection
      try {
         byte[] wrappedBytes = ProtobufUtil.toWrappedByteArray(serCtx, existingValue);
         byte[] updatedWrapped = applyProtobufUpdateToBytes(wrappedBytes, serCtx, ops);
         if (updatedWrapped == null) return false;
         Object updatedObject = ProtobufUtil.fromWrappedByteArray(serCtx, updatedWrapped);
         cache.put(key, updatedObject);
         return true;
      } catch (IllegalArgumentException e) {
         // No ProtoStream marshaller registered — fall back to reflection
         return applyReflectionUpdate(cache, key, existingValue, ops);
      }
   }

   private static boolean applyProtobufUpdate(AdvancedCache<Object, Object> storageCache, Object key,
                                               Object existingValue, ImmutableSerializationContext serCtx,
                                               List<ProtobufFieldUpdater.UpdateOperation> ops) throws IOException {
      byte[] wrappedBytes = existingValue instanceof byte[] b ? b
            : ((WrappedByteArray) existingValue).getBytes();
      byte[] updatedWrapped = applyProtobufUpdateToBytes(wrappedBytes, serCtx, ops);
      if (updatedWrapped == null) return false;
      storageCache.put(key, updatedWrapped);
      return true;
   }

   private static byte[] applyProtobufUpdateToBytes(byte[] wrappedBytes, ImmutableSerializationContext serCtx,
                                                     List<ProtobufFieldUpdater.UpdateOperation> ops) throws IOException {
      String typeName = null;
      Integer typeId = null;
      byte[] innerBytes = null;

      TagReaderImpl reader = TagReaderImpl.newInstance(serCtx, wrappedBytes);
      int tag;
      while ((tag = reader.readTag()) != 0) {
         int fieldNumber = WireType.getTagFieldNumber(tag);
         if (fieldNumber == WrappedMessage.WRAPPED_TYPE_NAME) {
            typeName = reader.readString();
         } else if (fieldNumber == WrappedMessage.WRAPPED_TYPE_ID) {
            typeId = reader.readUInt32();
         } else if (fieldNumber == WrappedMessage.WRAPPED_MESSAGE) {
            innerBytes = reader.readByteArray();
         } else {
            reader.skipField(tag);
         }
      }

      if (innerBytes == null) return null;

      String resolvedTypeName = typeName != null ? typeName
            : serCtx.getDescriptorByTypeId(typeId).getFullName();
      Descriptor descriptor = serCtx.getMessageDescriptor(resolvedTypeName);

      byte[] updatedInner = ProtobufFieldUpdater.update(descriptor, innerBytes, ops);
      return rewrap(serCtx, typeName, typeId, updatedInner);
   }

   @SuppressWarnings("unchecked")
   private static boolean applyReflectionUpdate(AdvancedCache<Object, Object> cache, Object key,
                                                 Object value,
                                                 List<ProtobufFieldUpdater.UpdateOperation> ops) {
      for (ProtobufFieldUpdater.UpdateOperation op : ops) {
         String[] path = op.propertyPath();
         List<Object> values = op.values();
         try {
            Object target = value;
            for (int i = 0; i < path.length - 1; i++) {
               target = getPropertyValue(target, path[i]);
            }
            String fieldName = path[path.length - 1];
            switch (op.type()) {
               case SET -> {
                  Object newValue = values != null && !values.isEmpty() ? values.get(0) : null;
                  setPropertyValue(target, fieldName, newValue);
               }
               case ADD -> {
                  Collection<Object> collection = (Collection<Object>) getPropertyValue(target, fieldName);
                  if (collection != null && values != null) {
                     collection.addAll(values);
                  }
               }
               case REMOVE -> {
                  Collection<Object> collection = (Collection<Object>) getPropertyValue(target, fieldName);
                  if (collection != null && values != null) {
                     collection.removeAll(values);
                  }
               }
            }
         } catch (Exception e) {
            throw CONTAINER.updateByQueryFailed(String.join(".", path), e);
         }
      }
      cache.put(key, value);
      return true;
   }

   private static Object getPropertyValue(Object obj, String propertyName) throws Exception {
      Method getter = findGetter(obj.getClass(), propertyName);
      if (getter != null) {
         return getter.invoke(obj);
      }
      Field field = findField(obj.getClass(), propertyName);
      field.setAccessible(true);
      return field.get(obj);
   }

   private static void setPropertyValue(Object obj, String propertyName, Object value) throws Exception {
      Method setter = findSetter(obj.getClass(), propertyName);
      if (setter != null) {
         setter.invoke(obj, convertValue(value, setter.getParameterTypes()[0]));
         return;
      }
      Field field = findField(obj.getClass(), propertyName);
      field.setAccessible(true);
      field.set(obj, convertValue(value, field.getType()));
   }

   private static Method findGetter(Class<?> clazz, String propertyName) {
      String capitalized = Character.toUpperCase(propertyName.charAt(0)) + propertyName.substring(1);
      try {
         return clazz.getMethod("get" + capitalized);
      } catch (NoSuchMethodException e) {
         try {
            return clazz.getMethod("is" + capitalized);
         } catch (NoSuchMethodException e2) {
            return null;
         }
      }
   }

   private static Method findSetter(Class<?> clazz, String propertyName) {
      String capitalized = Character.toUpperCase(propertyName.charAt(0)) + propertyName.substring(1);
      String setterName = "set" + capitalized;
      for (Method m : clazz.getMethods()) {
         if (m.getName().equals(setterName) && m.getParameterCount() == 1) {
            return m;
         }
      }
      return null;
   }

   private static Field findField(Class<?> clazz, String name) throws NoSuchFieldException {
      Class<?> current = clazz;
      while (current != null) {
         try {
            return current.getDeclaredField(name);
         } catch (NoSuchFieldException e) {
            current = current.getSuperclass();
         }
      }
      throw new NoSuchFieldException(name + " in " + clazz.getName());
   }

   private static Object convertValue(Object value, Class<?> targetType) {
      if (value == null) return null;
      if (targetType.isInstance(value)) return value;

      String str = value.toString();
      if (targetType == String.class) return str;
      if (targetType == int.class || targetType == Integer.class) return Integer.parseInt(str);
      if (targetType == long.class || targetType == Long.class) return Long.parseLong(str);
      if (targetType == double.class || targetType == Double.class) return Double.parseDouble(str);
      if (targetType == float.class || targetType == Float.class) return Float.parseFloat(str);
      if (targetType == boolean.class || targetType == Boolean.class) return Boolean.parseBoolean(str);
      return value;
   }

   private static byte[] rewrap(ImmutableSerializationContext ctx, String typeName, Integer typeId, byte[] innerBytes) throws IOException {
      var baos = new RandomAccessOutputStreamImpl(innerBytes.length + 20);
      var writer = TagWriterImpl.newInstance(ctx, (org.infinispan.protostream.RandomAccessOutputStream) baos);

      if (typeId != null) {
         writer.writeUInt32(WrappedMessage.WRAPPED_TYPE_ID, typeId);
      } else if (typeName != null) {
         writer.writeString(WrappedMessage.WRAPPED_TYPE_NAME, typeName);
      }
      writer.writeBytes(WrappedMessage.WRAPPED_MESSAGE, innerBytes);
      writer.flush();
      return baos.toByteArray();
   }
}
