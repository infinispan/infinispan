package org.infinispan.query.core.impl;

import static org.infinispan.query.core.impl.Log.CONTAINER;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.ProtobufFieldUpdater;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.WireType;
import org.infinispan.protostream.impl.RandomAccessOutputStreamImpl;
import org.infinispan.protostream.impl.TagReaderImpl;
import org.infinispan.protostream.impl.TagWriterImpl;
import org.infinispan.query.objectfilter.impl.syntax.ConstantValueExpr;
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

   public enum UpdateStrategy {
      PROTOBUF,
      PROTOBUF_ROUNDTRIP,
      REFLECTION
   }

   private UpdateQueryHelper() {
   }

   public static UpdateStrategy resolveStrategy(AdvancedCache<?, ?> cache,
                                                  ImmutableSerializationContext serCtx,
                                                  String targetEntityName) {
      MediaType storageType = cache.getValueDataConversion().getStorageMediaType();
      if (storageType.match(MediaType.APPLICATION_PROTOSTREAM)) {
         return UpdateStrategy.PROTOBUF;
      }
      if (serCtx.canMarshall(targetEntityName)) {
         return UpdateStrategy.PROTOBUF_ROUNDTRIP;
      }
      return UpdateStrategy.REFLECTION;
   }

   public static List<ProtobufFieldUpdater.UpdateOperation> toProtobufOps(
         List<IckleParsingResult.UpdateOperation> updateOperations, Map<String, Object> namedParameters) {
      List<ProtobufFieldUpdater.UpdateOperation> ops = new ArrayList<>();
      for (IckleParsingResult.UpdateOperation uo : updateOperations) {
         ProtobufFieldUpdater.OperationType opType = switch (uo.getType()) {
            case SET -> ProtobufFieldUpdater.OperationType.SET;
            case ADD -> ProtobufFieldUpdater.OperationType.ADD;
            case REMOVE -> ProtobufFieldUpdater.OperationType.REMOVE;
         };
         List<Object> resolvedValues = resolveValues(uo.getValues(), namedParameters);
         ops.add(new ProtobufFieldUpdater.UpdateOperation(opType, uo.getPropertyPath(), resolvedValues));
      }
      return ops;
   }

   /**
    * Applies an update to a cache entry atomically using {@code cache.compute()}.
    * The {@link UpdateBiFunction} is marshallable and safe for clustered (backup replication) use.
    */
   public static boolean applyUpdate(AdvancedCache<Object, Object> cache, Object key, UpdateBiFunction fn) {
      cache.withStorageMediaType().compute(key, fn);
      return fn.wasUpdated();
   }

   private static List<Object> resolveValues(List<Object> values, Map<String, Object> namedParameters) {
      if (values == null || namedParameters == null || namedParameters.isEmpty()) {
         return values;
      }
      List<Object> resolved = new ArrayList<>(values.size());
      for (Object value : values) {
         if (value instanceof ConstantValueExpr.ParamPlaceholder placeholder) {
            Object paramValue = namedParameters.get(placeholder.getName());
            if (paramValue == null && !namedParameters.containsKey(placeholder.getName())) {
               throw new IllegalArgumentException("Missing value for parameter: " + placeholder.getName());
            }
            resolved.add(paramValue);
         } else {
            resolved.add(value);
         }
      }
      return resolved;
   }

   /**
    * A marshallable BiFunction for use with {@code cache.compute()}.
    * Carries the query string and named parameters; resolves update operations
    * and serialization context via dependency injection on each node.
    * <p>
    * Follows the same pattern as {@link org.infinispan.query.core.impl.eventfilter.IckleFilterAndConverter}
    * and {@link EmbeddedQuery.DeleteFunction}.
    *
    * @since 16.3
    */
   @ProtoTypeId(ProtoStreamTypeIds.ICKLE_UPDATE_BI_FUNCTION)
   @Scope(Scopes.NONE)
   public static final class UpdateBiFunction implements BiFunction<Object, Object, Object> {

      private final String queryString;
      private final Map<String, Object> namedParameters;
      private final String targetEntityName;

      private transient List<ProtobufFieldUpdater.UpdateOperation> ops;
      private transient ImmutableSerializationContext serCtx;
      private transient UpdateStrategy strategy;
      private transient boolean updated;

      public UpdateBiFunction(String queryString, Map<String, Object> namedParameters, String targetEntityName) {
         this.queryString = queryString;
         this.namedParameters = namedParameters;
         this.targetEntityName = targetEntityName;
      }

      @ProtoFactory
      UpdateBiFunction(String queryString, MarshallableMap<String, Object> wrappedNamedParameters,
                       String targetEntityName) {
         this(queryString, MarshallableMap.unwrap(wrappedNamedParameters), targetEntityName);
      }

      @ProtoField(1)
      public String getQueryString() {
         return queryString;
      }

      @ProtoField(2)
      public MarshallableMap<String, Object> getWrappedNamedParameters() {
         return MarshallableMap.create(namedParameters);
      }

      @ProtoField(3)
      public String getTargetEntityName() {
         return targetEntityName;
      }

      @Inject
      void injectDependencies(ComponentRegistry componentRegistry) {
         if (ops != null) return;

         AdvancedCache<?, ?> cache = componentRegistry.getCache().wired().getAdvancedCache();
         SerializationContextRegistry ctxRegistry = componentRegistry.getComponent(SerializationContextRegistry.class);
         serCtx = ctxRegistry.getUserCtx();

         QueryEngine<?> queryEngine = componentRegistry.getComponent(QueryEngine.class);
         IckleParsingResult<?> parsingResult = queryEngine.parse(queryString);
         ops = toProtobufOps(parsingResult.getUpdateOperations(), namedParameters);
         String entityName = targetEntityName != null ? targetEntityName : parsingResult.getTargetEntityName();
         strategy = resolveStrategy(cache, serCtx, entityName);
      }

      public boolean wasUpdated() {
         return updated;
      }

      @Override
      public Object apply(Object key, Object existingValue) {
         updated = false;
         if (existingValue == null) return null;

         try {
            return switch (strategy) {
               case PROTOBUF -> {
                  byte[] wrappedBytes = existingValue instanceof byte[] b ? b
                        : ((WrappedByteArray) existingValue).getBytes();
                  byte[] updatedWrapped = applyProtobufUpdateToBytes(wrappedBytes, serCtx, ops);
                  if (updatedWrapped == null) yield existingValue;
                  updated = true;
                  yield updatedWrapped;
               }
               case PROTOBUF_ROUNDTRIP -> {
                  byte[] wrappedBytes = ProtobufUtil.toWrappedByteArray(serCtx, existingValue);
                  byte[] updatedWrapped = applyProtobufUpdateToBytes(wrappedBytes, serCtx, ops);
                  if (updatedWrapped == null) yield existingValue;
                  updated = true;
                  yield ProtobufUtil.fromWrappedByteArray(serCtx, updatedWrapped);
               }
               case REFLECTION -> applyReflectionOps(existingValue);
            };
         } catch (IOException e) {
            throw CONTAINER.updateByQueryFailed(key, e);
         }
      }

      @SuppressWarnings("unchecked")
      private Object applyReflectionOps(Object value) {
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
                     if (collection == null || values == null) return value;
                     collection.addAll(values);
                  }
                  case REMOVE -> {
                     Collection<Object> collection = (Collection<Object>) getPropertyValue(target, fieldName);
                     if (collection == null || values == null) return value;
                     collection.removeAll(values);
                  }
               }
            } catch (Exception e) {
               throw CONTAINER.updateByQueryFailed(String.join(".", path), e);
            }
         }
         updated = true;
         return value;
      }
   }

   static byte[] applyProtobufUpdateToBytes(byte[] wrappedBytes, ImmutableSerializationContext serCtx,
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
      return Util.fromString(targetType, value.toString());
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
