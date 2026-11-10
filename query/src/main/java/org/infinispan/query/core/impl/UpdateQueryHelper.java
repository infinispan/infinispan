package org.infinispan.query.core.impl;

import static org.infinispan.query.core.impl.Log.CONTAINER;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;

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
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.ProtobufFieldUpdater;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.TagHandler;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.Type;
import org.infinispan.protostream.descriptors.WireType;
import org.infinispan.protostream.impl.RandomAccessOutputStreamImpl;
import org.infinispan.protostream.impl.TagReaderImpl;
import org.infinispan.protostream.impl.TagWriterImpl;
import org.infinispan.query.functions.QueryFunctionContext;
import org.infinispan.query.impl.QueryEngine;
import org.infinispan.query.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.query.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.objectfilter.impl.syntax.update.IntegerRange;
import org.infinispan.query.objectfilter.impl.syntax.update.QueryFunctionRegistry;
import org.infinispan.query.objectfilter.impl.syntax.update.UpdateValueEvaluator;
import org.infinispan.query.objectfilter.impl.syntax.update.UpdateValueExpr;
import org.infinispan.util.function.SerializableFunction;

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
    * Returns {@code true} if the entry existed (and was therefore updated), {@code false} otherwise.
    * <p>
    * The result is derived from the compute return value rather than {@link UpdateBiFunction#wasUpdated()}
    * because the function may be serialized and executed on a remote node, leaving the local instance's
    * state unchanged.
    */
   public static boolean applyUpdate(AdvancedCache<Object, Object> cache, Object key, UpdateBiFunction fn) {
      Object result = cache.withStorageMediaType().compute(key, fn);
      return result != null;
   }

   /**
    * Asynchronous variant of {@link #applyUpdate(AdvancedCache, Object, UpdateBiFunction)} using
    * {@code cache.computeAsync()}. Returns a future completing with {@code true} if the entry existed
    * (and was therefore updated), {@code false} otherwise.
    * <p>
    * The function instance may be shared across concurrent computes; the result is derived from
    * the compute return value, not from per-instance mutable state.
    */
   public static CompletableFuture<Boolean> applyUpdateAsync(AdvancedCache<Object, Object> cache, Object key,
                                                             UpdateBiFunction fn) {
      return cache.withStorageMediaType().computeAsync(key, fn).thenApply(Objects::nonNull);
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
      private final SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>> engineProvider;

      // Injected lazily on first use and shared across (potentially concurrent) computes.
      // The volatile fields make the one-time injection visible to all threads; {@code ops}
      // doubles as the "already injected" flag and is written last so it publishes the others.
      private transient volatile List<ProtobufFieldUpdater.UpdateOperation> ops;
      private transient volatile ImmutableSerializationContext serCtx;
      private transient volatile UpdateStrategy strategy;
      private transient volatile boolean hasExpressions;
      private transient volatile List<IckleParsingResult.UpdateOperation> parsedUpdateOps;
      private transient volatile Map<String, Pattern> patternCache;
      private transient volatile QueryFunctionRegistry functionRegistry;
      private transient volatile ReflectionResolver resolver;

      // Set by {@link #apply} to indicate whether the entry was actually modified.
      // Used by the synchronous {@link #applyUpdate} path via {@link #wasUpdated()}.
      // The async path uses a value-based check instead and never reads this field.
      private transient volatile boolean updated;

      public UpdateBiFunction(String queryString, Map<String, Object> namedParameters, String targetEntityName,
                              SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>> engineProvider) {
         this.queryString = queryString;
         this.namedParameters = namedParameters;
         this.targetEntityName = targetEntityName;
         this.engineProvider = engineProvider;
      }

      @ProtoFactory
      UpdateBiFunction(String queryString, MarshallableMap<String, Object> wrappedNamedParameters,
                       String targetEntityName,
                       MarshallableObject<SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>>> wrappedEngineProvider) {
         this(queryString, MarshallableMap.unwrap(wrappedNamedParameters), targetEntityName,
               MarshallableObject.unwrap(wrappedEngineProvider));
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

      @ProtoField(4)
      public MarshallableObject<SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>>> getWrappedEngineProvider() {
         return MarshallableObject.create(engineProvider);
      }

      @Inject
      void injectDependencies(ComponentRegistry componentRegistry) {
         if (ops != null) return;

         AdvancedCache<?, ?> cache = componentRegistry.getCache().wired().getAdvancedCache();
         SerializationContextRegistry ctxRegistry = componentRegistry.getComponent(SerializationContextRegistry.class);
         ImmutableSerializationContext context = ctxRegistry.getUserCtx();

         QueryEngine<?> queryEngine = engineProvider != null
               ? engineProvider.apply(cache)
               : componentRegistry.getComponent(QueryEngine.class);
         IckleParsingResult<?> parsingResult = queryEngine.parse(queryString);
         List<IckleParsingResult.UpdateOperation> rawOps = parsingResult.getUpdateOperations();
         parsedUpdateOps = rawOps;
         hasExpressions = rawOps.stream()
               .flatMap(uo -> uo.getValues().stream())
               .anyMatch(v -> v instanceof UpdateValueExpr);
         if (hasExpressions) {
            patternCache = new ConcurrentHashMap<>();
            functionRegistry = componentRegistry.getGlobalComponentRegistry().getComponent(QueryFunctionRegistry.class);
            for (IckleParsingResult.UpdateOperation uo : rawOps) {
               for (Object v : uo.getValues()) {
                  if (v instanceof UpdateValueExpr expr) {
                     expr.prepare(functionRegistry, patternCache);
                  }
               }
            }
         }
         List<ProtobufFieldUpdater.UpdateOperation> parsedOps = toProtobufOps(rawOps, namedParameters);
         String entityName = targetEntityName != null ? targetEntityName : parsingResult.getTargetEntityName();
         UpdateStrategy resolvedStrategy = resolveStrategy(cache, context, entityName);

         serCtx = context;
         strategy = resolvedStrategy;
         resolver = new ReflectionResolver();
         ops = parsedOps;
      }

      public boolean wasUpdated() {
         return updated;
      }

      @Override
      public Object apply(Object key, Object existingValue) {
         updated = false;
         if (existingValue == null) return null;

         try {
            if (hasExpressions) {
               return applyExpressionUpdate(key, existingValue);
            }
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

      private Object applyExpressionUpdate(Object key, Object existingValue) throws IOException {
         if (strategy == UpdateStrategy.PROTOBUF) {
            byte[] wrappedBytes = existingValue instanceof byte[] b ? b
                  : ((WrappedByteArray) existingValue).getBytes();

            String typeName = null;
            Integer typeId = null;
            byte[] innerBytes = null;
            TagReaderImpl reader = TagReaderImpl.newInstance(serCtx, wrappedBytes);
            int tag;
            while ((tag = reader.readTag()) != 0) {
               int fieldNumber = WireType.getTagFieldNumber(tag);
               if (fieldNumber == WrappedMessage.WRAPPED_TYPE_NAME) typeName = reader.readString();
               else if (fieldNumber == WrappedMessage.WRAPPED_TYPE_ID) typeId = reader.readUInt32();
               else if (fieldNumber == WrappedMessage.WRAPPED_MESSAGE) innerBytes = reader.readByteArray();
               else reader.skipField(tag);
            }
            if (innerBytes == null) throw new IOException("No message content in wrapped bytes for expression update");

            String resolvedTypeName = typeName != null ? typeName
                  : serCtx.getDescriptorByTypeId(typeId).getFullName();
            Descriptor descriptor = serCtx.getMessageDescriptor(resolvedTypeName);
            ProtobufFieldAccessor accessor = new ProtobufFieldAccessor(serCtx, descriptor, innerBytes);

            QueryFunctionContext evalCtx = new QueryFunctionContext(
                  null,
                  path -> {
                     try {
                        return accessor.readFieldPath(path);
                     } catch (Exception e) {
                        throw new IllegalArgumentException("Cannot read property path: " + String.join(".", path), e);
                     }
                  },
                  namedParameters,
                  functionRegistry,
                  patternCache);

            List<List<Object>> evaluatedOps = evaluateAllOperations(evalCtx);
            for (int i = 0; i < parsedUpdateOps.size(); i++) {
               accessor.applyOperation(parsedUpdateOps.get(i), evaluatedOps.get(i));
            }

            updated = true;
            return rewrap(serCtx, typeName, typeId, accessor.toBytes());
         }

         Object entity = existingValue;
         QueryFunctionContext evalCtx = new QueryFunctionContext(
               entity,
               path -> {
                  try {
                     return resolver.readNestedProperty(entity, path);
                  } catch (Exception e) {
                     throw new IllegalArgumentException("Cannot read property path: " + String.join(".", path), e);
                  }
               },
               namedParameters,
               functionRegistry,
               patternCache);

         List<List<Object>> evaluatedOps = evaluateAllOperations(evalCtx);
         List<Runnable> undo = new ArrayList<>();
         try {
            for (int i = 0; i < parsedUpdateOps.size(); i++) {
               try {
                  applyOperationToEntity(entity, parsedUpdateOps.get(i), evaluatedOps.get(i), undo);
               } catch (Exception e) {
                  throw new IllegalArgumentException("Failed to apply update operation to " + String.join(".", parsedUpdateOps.get(i).getPropertyPath()), e);
               }
            }
            updated = true;
            return entity;
         } catch (Exception e) {
            // Roll back the partial mutations so a failing statement leaves the entry untouched.
            for (int i = undo.size() - 1; i >= 0; i--) {
               undo.get(i).run();
            }
            throw e;
         }
      }

      /**
       * Evaluates the value expressions of every update operation against the
       * original, unmodified entity. Computing all right-hand sides before any
       * assignment is applied gives the pre-image semantics of a SQL
       * {@code UPDATE}: in {@code set a = a + 1, set b = a * 2}, both
       * right-hand sides are evaluated using the original value of {@code a}.
       */
      private List<List<Object>> evaluateAllOperations(QueryFunctionContext evalCtx) {
         List<List<Object>> evaluated = new ArrayList<>(parsedUpdateOps.size());
         for (IckleParsingResult.UpdateOperation uo : parsedUpdateOps) {
            List<Object> evaluatedValues = new ArrayList<>(uo.getValues().size());
            for (Object v : uo.getValues()) {
               evaluatedValues.add(UpdateValueEvaluator.evaluateOperand(v, evalCtx));
            }
            evaluated.add(evaluatedValues);
         }
         return evaluated;
      }

      @SuppressWarnings("unchecked")
      private void applyOperationToEntity(Object entity, IckleParsingResult.UpdateOperation uo, List<Object> values,
                                          List<Runnable> undo) throws Exception {
         String[] path = uo.getPropertyPath();
         Object target = entity;
         for (int i = 0; i < path.length - 1; i++) {
            target = resolver.getPropertyValue(target, path[i]);
         }
         String fieldName = path[path.length - 1];
         switch (uo.getType()) {
            case SET -> {
               Object oldValue = resolver.getPropertyValue(target, fieldName);
               undo.add(restoreField(target, fieldName, oldValue));
               resolver.setPropertyValue(target, fieldName, values.get(0));
            }
            case ADD -> {
               Collection<Object> collection = (Collection<Object>) resolver.getPropertyValue(target, fieldName);
               if (collection != null) {
                  List<Object> saved = new ArrayList<>(collection);
                  undo.add(restoreCollection(collection, saved));
                  collection.addAll(values);
               }
            }
            case REMOVE -> {
               Collection<Object> collection = (Collection<Object>) resolver.getPropertyValue(target, fieldName);
               if (collection != null) {
                  List<Object> saved = new ArrayList<>(collection);
                  undo.add(restoreCollection(collection, saved));
                  collection.removeAll(values);
               }
            }
         }
      }

      @SuppressWarnings("unchecked")
      private Object applyReflectionOps(Object value) {
         List<Runnable> undo = new ArrayList<>();
         try {
            for (ProtobufFieldUpdater.UpdateOperation op : ops) {
               String[] path = op.propertyPath();
               List<Object> values = op.values();
               try {
                  Object target = value;
                  for (int i = 0; i < path.length - 1; i++) {
                     target = resolver.getPropertyValue(target, path[i]);
                  }
                  String fieldName = path[path.length - 1];
                  switch (op.type()) {
                     case SET -> {
                        Object oldValue = resolver.getPropertyValue(target, fieldName);
                        undo.add(restoreField(target, fieldName, oldValue));
                        Object newValue = values != null && !values.isEmpty() ? values.get(0) : null;
                        resolver.setPropertyValue(target, fieldName, newValue);
                        updated = true;
                     }
                     case ADD -> {
                        Collection<Object> collection = (Collection<Object>) resolver.getPropertyValue(target, fieldName);
                        if (collection == null || values == null) return value;
                        List<Object> saved = new ArrayList<>(collection);
                        undo.add(restoreCollection(collection, saved));
                        collection.addAll(values);
                        updated = true;
                     }
                     case REMOVE -> {
                        Collection<Object> collection = (Collection<Object>) resolver.getPropertyValue(target, fieldName);
                        if (collection == null || values == null) return value;
                        List<Object> saved = new ArrayList<>(collection);
                        undo.add(restoreCollection(collection, saved));
                        collection.removeAll(values);
                        updated = true;
                     }
                  }
               } catch (Exception e) {
                  throw CONTAINER.updateByQueryFailed(String.join(".", path), e);
               }
            }
            return value;
         } catch (Exception e) {
            // Roll back the partial mutations so a failing statement leaves the entry untouched.
            for (int i = undo.size() - 1; i >= 0; i--) {
               undo.get(i).run();
            }
            throw e;
         }
      }

      /**
       * Captures the pre-mutation state of a single field and returns an action
       * that restores it. Used to roll back partial mutations when a multi-field
       * update statement fails, so the entry is left untouched.
       */
      private Runnable restoreField(Object target, String fieldName, Object oldValue) {
         return () -> {
            try {
               resolver.setPropertyValue(target, fieldName, oldValue);
            } catch (Exception e) {
               throw new IllegalStateException("Failed to restore field " + fieldName, e);
            }
         };
      }
   }

   private static final class ProtobufFieldAccessor {
      private final ImmutableSerializationContext serCtx;
      private final Descriptor descriptor;
      private final Map<Integer, List<Object>> fieldMap;

      ProtobufFieldAccessor(ImmutableSerializationContext serCtx, Descriptor descriptor, byte[] innerBytes) throws IOException {
         this.serCtx = serCtx;
         this.descriptor = descriptor;
         this.fieldMap = collectFields(descriptor, innerBytes);
      }

      Object readFieldPath(String[] path) throws IOException {
         Descriptor currentDescriptor = descriptor;
         Map<Integer, List<Object>> currentMap = fieldMap;
         for (int i = 0; i < path.length; i++) {
            String fieldName = path[i];
            FieldDescriptor fd = currentDescriptor.findFieldByName(fieldName);
            if (fd == null) {
               throw new IllegalArgumentException("Unknown field '" + fieldName + "' in message " + currentDescriptor.getFullName());
            }
            List<Object> values = currentMap.get(fd.getNumber());
            if (values == null || values.isEmpty()) return null;
            if (i == path.length - 1) {
               if (fd.getType() == Type.MESSAGE) {
                  String nestedTypeName = fd.getMessageType().getFullName();
                  List<Object> decoded = new ArrayList<>(values.size());
                  for (Object v : values) {
                     decoded.add(v instanceof byte[] b ? deserializeMessage(nestedTypeName, b) : v);
                  }
                  return fd.isRepeated() ? decoded : decoded.get(0);
               }
               return fd.isRepeated() ? values : values.get(0);
            }
            if (fd.getType() != Type.MESSAGE) {
               throw new IllegalArgumentException("Field '" + fieldName + "' is not a message type, cannot traverse into it");
            }
            byte[] nestedBytes = (byte[]) values.get(0);
            currentDescriptor = fd.getMessageType();
            currentMap = collectFields(currentDescriptor, nestedBytes);
         }
         return null;
      }

      void applyOperation(IckleParsingResult.UpdateOperation uo, List<Object> values) throws IOException {
         String[] path = uo.getPropertyPath();
         switch (uo.getType()) {
            case SET -> applySet(path, values.isEmpty() ? null : values.get(0));
            case ADD -> applyAddRemove(path, values, true);
            case REMOVE -> applyAddRemove(path, values, false);
         }
      }

      void applyOperation(ProtobufFieldUpdater.OperationType type, String[] path, List<Object> values) throws IOException {
         switch (type) {
            case SET -> applySet(path, values.isEmpty() ? null : values.get(0));
            case ADD -> applyAddRemove(path, values, true);
            case REMOVE -> applyAddRemove(path, values, false);
         }
      }

      byte[] toBytes() throws IOException {
         return serializeFields(descriptor, fieldMap);
      }

      private void applySet(String[] path, Object value) throws IOException {
         Descriptor currentDescriptor = descriptor;
         Map<Integer, List<Object>> currentMap = fieldMap;
         for (int i = 0; i < path.length - 1; i++) {
            FieldDescriptor fd = currentDescriptor.findFieldByName(path[i]);
            if (fd == null)
               throw new IllegalArgumentException("Unknown field '" + path[i] + "' in " + currentDescriptor.getFullName());
            List<Object> values = currentMap.get(fd.getNumber());
            if (values == null || values.isEmpty()) {
               currentMap.computeIfAbsent(fd.getNumber(), k -> new ArrayList<>()).add(new byte[0]);
               values = currentMap.get(fd.getNumber());
            }
            byte[] nestedBytes = (byte[]) values.get(0);
            currentDescriptor = fd.getMessageType();
            Map<Integer, List<Object>> nestedMap = collectFields(currentDescriptor, nestedBytes);
            byte[] updatedNested = serializeFields(currentDescriptor, nestedMap);
            values.set(0, updatedNested);
            currentDescriptor = fd.getMessageType();
            currentMap = nestedMap;
         }
         FieldDescriptor leafFd = currentDescriptor.findFieldByName(path[path.length - 1]);
         if (leafFd == null)
            throw new IllegalArgumentException("Unknown field '" + path[path.length - 1] + "' in " + currentDescriptor.getFullName());
         if (value == null) {
            currentMap.remove(leafFd.getNumber());
         } else {
            currentMap.put(leafFd.getNumber(), new ArrayList<>(List.of(convertFieldValue(leafFd, value))));
         }
      }

      private void applyAddRemove(String[] path, List<Object> values, boolean isAdd) throws IOException {
         Descriptor currentDescriptor = descriptor;
         Map<Integer, List<Object>> currentMap = fieldMap;
         for (int i = 0; i < path.length - 1; i++) {
            FieldDescriptor fd = currentDescriptor.findFieldByName(path[i]);
            if (fd == null)
               throw new IllegalArgumentException("Unknown field '" + path[i] + "' in " + currentDescriptor.getFullName());
            List<Object> existing = currentMap.get(fd.getNumber());
            if (existing == null || existing.isEmpty()) return;
            byte[] nestedBytes = (byte[]) existing.get(0);
            currentDescriptor = fd.getMessageType();
            Map<Integer, List<Object>> nestedMap = collectFields(currentDescriptor, nestedBytes);
            byte[] updatedNested = serializeFields(currentDescriptor, nestedMap);
            existing.set(0, updatedNested);
            currentMap = nestedMap;
         }
         FieldDescriptor leafFd = currentDescriptor.findFieldByName(path[path.length - 1]);
         if (leafFd == null)
            throw new IllegalArgumentException("Unknown field '" + path[path.length - 1] + "' in " + currentDescriptor.getFullName());
         List<Object> existing = currentMap.computeIfAbsent(leafFd.getNumber(), k -> new ArrayList<>());
         if (isAdd) {
            for (Object v : values) {
               existing.add(convertFieldValue(leafFd, v));
            }
         } else {
            for (Object v : values) {
               Object converted = convertFieldValue(leafFd, v);
               existing.removeIf(e -> Objects.equals(e, converted));
            }
            if (existing.isEmpty()) currentMap.remove(leafFd.getNumber());
         }
      }

      private static Map<Integer, List<Object>> collectFields(Descriptor descriptor, byte[] bytes) throws IOException {
         FieldCollector collector = new FieldCollector();
         ProtobufParser.INSTANCE.parse(collector, descriptor, bytes);
         return collector.fields;
      }

      private static byte[] serializeFields(Descriptor descriptor, Map<Integer, List<Object>> fieldMap) throws IOException {
         var baos = new RandomAccessOutputStreamImpl(128);
         var writer = TagWriterImpl.newInstance(null, (org.infinispan.protostream.RandomAccessOutputStream) baos);
         for (var entry : fieldMap.entrySet()) {
            int fieldNumber = entry.getKey();
            List<Object> values = entry.getValue();
            FieldDescriptor fd = descriptor.findFieldByNumber(fieldNumber);
            for (Object value : values) {
               if (value instanceof byte[] b) {
                  writer.writeBytes(fieldNumber, b);
               } else if (fd != null) {
                  writeScalarField(writer, fieldNumber, fd, value);
               }
            }
         }
         writer.flush();
         return baos.toByteArray();
      }

      private static void writeScalarField(org.infinispan.protostream.TagWriter writer, int fieldNumber, FieldDescriptor fd, Object value) throws IOException {
         if (value == null) return;
         switch (fd.getType()) {
            case STRING -> writer.writeString(fieldNumber, (String) value);
            case INT32, UINT32 -> writer.writeInt32(fieldNumber, requireIntegral(((Number) value), "int32").intValue());
            case SINT32 -> writer.writeSInt32(fieldNumber, requireIntegral(((Number) value), "int32").intValue());
            case FIXED32 -> writer.writeFixed32(fieldNumber, requireIntegral(((Number) value), "int32").intValue());
            case SFIXED32 -> writer.writeSFixed32(fieldNumber, requireIntegral(((Number) value), "int32").intValue());
            case INT64, UINT64 ->
                  writer.writeInt64(fieldNumber, requireIntegral(((Number) value), "int64").longValue());
            case SINT64 -> writer.writeSInt64(fieldNumber, requireIntegral(((Number) value), "int64").longValue());
            case FIXED64 -> writer.writeFixed64(fieldNumber, requireIntegral(((Number) value), "int64").longValue());
            case SFIXED64 -> writer.writeSFixed64(fieldNumber, requireIntegral(((Number) value), "int64").longValue());
            case FLOAT -> writer.writeFloat(fieldNumber, ((Number) value).floatValue());
            case DOUBLE -> writer.writeDouble(fieldNumber, ((Number) value).doubleValue());
            case BOOL -> writer.writeBool(fieldNumber, (Boolean) value);
            case ENUM -> writer.writeEnum(fieldNumber, ((Number) value).intValue());
            case BYTES -> writer.writeBytes(fieldNumber, (byte[]) value);
            default -> throw new IOException("Unsupported field type for writing: " + fd.getType());
         }
      }

      private Object convertFieldValue(FieldDescriptor fd, Object value) throws IOException {
         if (value == null) return null;
         if (fd.getType() == Type.MESSAGE) {
            return serializeMessage(value);
         }
         return switch (fd.getType()) {
            case STRING -> value instanceof String s ? s : value.toString();
            case INT32, SINT32, UINT32, FIXED32, SFIXED32 -> {
               if (value instanceof Number n) yield requireIntegral(n, "int32").intValue();
               yield Integer.parseInt(value.toString());
            }
            case INT64, SINT64, UINT64, FIXED64, SFIXED64 -> {
               if (value instanceof Number n) yield requireIntegral(n, "int64").longValue();
               yield Long.parseLong(value.toString());
            }
            case FLOAT -> {
               if (value instanceof Number n) yield n.floatValue();
               yield Float.parseFloat(value.toString());
            }
            case DOUBLE -> {
               if (value instanceof Number n) yield n.doubleValue();
               yield Double.parseDouble(value.toString());
            }
            case BOOL -> {
               if (value instanceof Boolean b) yield b;
               yield Boolean.parseBoolean(value.toString());
            }
            case ENUM -> {
               if (value instanceof Number n) yield n.intValue();
               var enumValue = fd.getEnumType().findValueByName(value.toString());
               if (enumValue == null) throw new IllegalArgumentException("Unknown enum value '" + value + "'");
               yield enumValue.getNumber();
            }
            default -> value;
         };
      }

      private byte[] serializeMessage(Object value) throws IOException {
         if (value instanceof byte[] b) {
            return b;
         }
         byte[] wrapped = ProtobufUtil.toWrappedByteArray(serCtx, value);
         return extractWrappedMessage(wrapped);
      }

      private Object deserializeMessage(String typeName, byte[] nestedBytes) throws IOException {
         if (nestedBytes == null || nestedBytes.length == 0) {
            return null;
         }
         return ProtobufUtil.fromWrappedByteArray(serCtx, rewrap(serCtx, typeName, null, nestedBytes));
      }

      private byte[] extractWrappedMessage(byte[] wrapped) throws IOException {
         TagReaderImpl reader = TagReaderImpl.newInstance(serCtx, wrapped);
         byte[] inner = null;
         int tag;
         while ((tag = reader.readTag()) != 0) {
            int fieldNumber = WireType.getTagFieldNumber(tag);
            if (fieldNumber == WrappedMessage.WRAPPED_MESSAGE) {
               inner = reader.readByteArray();
            } else {
               reader.skipField(tag);
            }
         }
         return inner;
      }

      private static final class FieldCollector implements TagHandler {
         final Map<Integer, List<Object>> fields = new LinkedHashMap<>();
         private int depth = 0;
         private int nestedFieldNumber = -1;
         private FieldCollector nestedCollector;
         private final org.infinispan.protostream.impl.RandomAccessOutputStreamImpl nestedBuffer = new org.infinispan.protostream.impl.RandomAccessOutputStreamImpl(64);
         private org.infinispan.protostream.TagWriter nestedWriter;

         @Override
         public void onTag(int fieldNumber, FieldDescriptor fieldDescriptor, Object tagValue) {
            if (depth > 0 && nestedCollector != null) {
               nestedCollector.onTag(fieldNumber, fieldDescriptor, tagValue);
               return;
            }
            fields.computeIfAbsent(fieldNumber, k -> new ArrayList<>()).add(tagValue);
         }

         @Override
         public void onStartNested(int fieldNumber, FieldDescriptor fieldDescriptor) {
            if (depth > 0 && nestedCollector != null) {
               nestedCollector.onStartNested(fieldNumber, fieldDescriptor);
               depth++;
               return;
            }
            depth++;
            nestedFieldNumber = fieldNumber;
            nestedCollector = new FieldCollector();
         }

         @Override
         public void onEndNested(int fieldNumber, FieldDescriptor fieldDescriptor) {
            depth--;
            if (depth > 0 && nestedCollector != null) {
               nestedCollector.onEndNested(fieldNumber, fieldDescriptor);
               return;
            }
            if (nestedCollector != null) {
               Descriptor nestedDescriptor = fieldDescriptor != null ? fieldDescriptor.getMessageType() : null;
               byte[] nestedBytes;
               try {
                  nestedBytes = serializeFields(nestedDescriptor, nestedCollector.fields);
               } catch (IOException e) {
                  throw new RuntimeException(e);
               }
               fields.computeIfAbsent(nestedFieldNumber, k -> new ArrayList<>()).add(nestedBytes);
               nestedCollector = null;
               nestedFieldNumber = -1;
            }
         }
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

      byte[] updatedInner;
      if (involvesMessageLeaf(descriptor, ops)) {
         ProtobufFieldAccessor accessor = new ProtobufFieldAccessor(serCtx, descriptor, innerBytes);
         for (ProtobufFieldUpdater.UpdateOperation op : ops) {
            accessor.applyOperation(op.type(), op.propertyPath(), op.values());
         }
         updatedInner = accessor.toBytes();
      } else {
         updatedInner = ProtobufFieldUpdater.update(descriptor, innerBytes, ops);
      }
      return rewrap(serCtx, typeName, typeId, updatedInner);
   }

   private static boolean involvesMessageLeaf(Descriptor descriptor, List<ProtobufFieldUpdater.UpdateOperation> ops) {
      for (ProtobufFieldUpdater.UpdateOperation op : ops) {
         if (isMessageLeaf(descriptor, op.propertyPath())) {
            return true;
         }
      }
      return false;
   }

   private static boolean isMessageLeaf(Descriptor descriptor, String[] path) {
      Descriptor current = descriptor;
      FieldDescriptor fd = null;
      for (int i = 0; i < path.length; i++) {
         fd = current.findFieldByName(path[i]);
         if (fd == null) {
            return false;
         }
         if (i == path.length - 1) {
            break;
         }
         if (fd.getType() != Type.MESSAGE) {
            return false;
         }
         current = fd.getMessageType();
      }
      return fd.getType() == Type.MESSAGE;
   }

   private static Runnable restoreCollection(Collection<Object> collection, List<Object> saved) {
      return () -> {
         collection.clear();
         collection.addAll(saved);
      };
   }

   /**
    * Resolves and caches the reflection members used to read and write entity properties.
    * Resolution is comparatively expensive ({@code getMethod}/{@code getMethods} scans and
    * exception-driven hierarchy walks) while the entity class and property names are constant
    * within a single statement or query, so resolved members are cached per (class, property)
    * and shared across all entries. The caches are populated lazily and are safe for
    * concurrent access; a failed resolution (no field anywhere in the hierarchy) is not
    * cached, so repeated attempts keep reporting the error.
    */
   public static final class ReflectionResolver {

      private final Map<Class<?>, Map<String, Optional<Method>>> getterCache = new ConcurrentHashMap<>();
      private final Map<Class<?>, Map<String, Optional<Method>>> setterCache = new ConcurrentHashMap<>();
      private final Map<Class<?>, Map<String, Field>> fieldCache = new ConcurrentHashMap<>();

      public Object getPropertyValue(Object obj, String propertyName) throws Exception {
         Method getter = cachedGetter(obj.getClass(), propertyName);
         if (getter != null) {
            return getter.invoke(obj);
         }
         return cachedField(obj.getClass(), propertyName).get(obj);
      }

      public void setPropertyValue(Object obj, String propertyName, Object value) throws Exception {
         Method setter = cachedSetter(obj.getClass(), propertyName);
         if (setter != null) {
            setter.invoke(obj, convertValue(value, setter.getParameterTypes()[0]));
            return;
         }
         Field field = cachedField(obj.getClass(), propertyName);
         field.set(obj, convertValue(value, field.getType()));
      }

      public Object readNestedProperty(Object entity, String[] path) throws Exception {
         Object target = entity;
         for (String segment : path) {
            target = getPropertyValue(target, segment);
         }
         return target;
      }

      private Method cachedGetter(Class<?> cls, String propertyName) {
         return getterCache.computeIfAbsent(cls, c -> new ConcurrentHashMap<>())
               .computeIfAbsent(propertyName, n -> Optional.ofNullable(findGetter(cls, n)))
               .orElse(null);
      }

      private Method cachedSetter(Class<?> cls, String propertyName) {
         return setterCache.computeIfAbsent(cls, c -> new ConcurrentHashMap<>())
               .computeIfAbsent(propertyName, n -> Optional.ofNullable(findSetter(cls, n)))
               .orElse(null);
      }

      private Field cachedField(Class<?> cls, String propertyName) throws NoSuchFieldException {
         try {
            return fieldCache.computeIfAbsent(cls, c -> new ConcurrentHashMap<>())
                  .computeIfAbsent(propertyName, n -> {
                     try {
                        Field field = findField(cls, n);
                        field.setAccessible(true);
                        return field;
                     } catch (NoSuchFieldException e) {
                        throw new FieldLookupFailure(e);
                     }
                  });
         } catch (FieldLookupFailure e) {
            throw (NoSuchFieldException) e.getCause();
         }
      }

      private static final class FieldLookupFailure extends RuntimeException {
         FieldLookupFailure(Throwable cause) {
            super(cause);
         }
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
         if (value instanceof Number n) {
            if (targetType == int.class || targetType == Integer.class)
               return requireIntegral(n, targetType.getSimpleName()).intValue();
            if (targetType == long.class || targetType == Long.class)
               return requireIntegral(n, targetType.getSimpleName()).longValue();
            if (targetType == double.class || targetType == Double.class) return n.doubleValue();
            if (targetType == float.class || targetType == Float.class) return n.floatValue();
            if (targetType == short.class || targetType == Short.class)
               return requireIntegral(n, targetType.getSimpleName()).shortValue();
            if (targetType == byte.class || targetType == Byte.class)
               return requireIntegral(n, targetType.getSimpleName()).byteValue();
         }
         if (targetType.isPrimitive()) {
            Class<?> wrapper = primitiveToWrapper(targetType);
            if (wrapper.isInstance(value)) return value;
            return Util.fromString(wrapper, value.toString());
         }
         return Util.fromString(targetType, value.toString());
      }

      private static Class<?> primitiveToWrapper(Class<?> primitive) {
         if (primitive == int.class) return Integer.class;
         if (primitive == long.class) return Long.class;
         if (primitive == double.class) return Double.class;
         if (primitive == float.class) return Float.class;
         if (primitive == short.class) return Short.class;
         if (primitive == byte.class) return Byte.class;
         if (primitive == boolean.class) return Boolean.class;
         if (primitive == char.class) return Character.class;
         return primitive;
      }
   }


   public static Function<String[], Object> createProtobufPropertyReader(ImmutableSerializationContext serCtx, byte[] wrappedBytes) throws IOException {
      String typeName = null;
      Integer typeId = null;
      byte[] innerBytes = null;
      TagReaderImpl reader = TagReaderImpl.newInstance(serCtx, wrappedBytes);
      int tag;
      while ((tag = reader.readTag()) != 0) {
         int fieldNumber = WireType.getTagFieldNumber(tag);
         if (fieldNumber == WrappedMessage.WRAPPED_TYPE_NAME) typeName = reader.readString();
         else if (fieldNumber == WrappedMessage.WRAPPED_TYPE_ID) typeId = reader.readUInt32();
         else if (fieldNumber == WrappedMessage.WRAPPED_MESSAGE) innerBytes = reader.readByteArray();
         else reader.skipField(tag);
      }
      if (innerBytes == null) throw new IOException("No message content in wrapped bytes");

      String resolvedTypeName = typeName != null ? typeName
            : serCtx.getDescriptorByTypeId(typeId).getFullName();
      Descriptor descriptor = serCtx.getMessageDescriptor(resolvedTypeName);
      ProtobufFieldAccessor accessor = new ProtobufFieldAccessor(serCtx, descriptor, innerBytes);

      return path -> {
         try {
            return accessor.readFieldPath(path);
         } catch (Exception e) {
            throw new IllegalArgumentException("Cannot read property path: " + String.join(".", path), e);
         }
      };
   }

   /**
    * Guards against narrowing a value into an integer type.
    * <p>
    * Expression evaluation may produce {@code double} results (e.g. {@code 7.0 / 2}) or
    * {@code long} results that exceed the target type (e.g. {@code 3000000000} into an
    * {@code int}); writing such a value would silently lose data, so non-integral and
    * out-of-range values are rejected with a clear error, while integral in-range
    * values (e.g. {@code 3.0}) convert normally.
    *
    * @param targetType the name of the target type: {@code int32}, {@code int64} or the simple
    *                   name of a Java integer type (e.g. {@code int}, {@code Short}, {@code byte})
    */
   private static Number requireIntegral(Number value, String targetType) {
      double dv = value.doubleValue();
      if (Double.isNaN(dv) || Double.isInfinite(dv) || dv != Math.floor(dv)) {
         throw new IllegalArgumentException("Cannot convert " + value + " to " + targetType + ": value is not integral");
      }
      if (!IntegerRange.fits(value, targetType)) {
         throw new IllegalArgumentException("Cannot convert " + value + " to " + targetType + ": value is out of range");
      }
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
