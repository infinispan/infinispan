package org.infinispan.server.resp.serialization;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.infinispan.server.resp.ByteBufPool;

/**
 * Entrypoint to serialize a response in RESP3 format.
 *
 * <p>
 * The class provides specific methods for the RESP3 types. In cases where the response object type is unknown or
 * heterogeneous during runtime, there is a generic method.
 * </p>
 *
 * <p>
 * During serialization, the class introspects the registry for a serializer capable of handling the object before
 * proceeding. Utilizing a specific method reduces the number of candidates to check. The generic method verifies all serializers
 * in the registry and short circuits after the first match. However, this could cause many checks until the correct serializer.
 * <b>Give preference to specific methods.</b>
 * </p>
 *
 * The method throws an exception when an object does not have a serializer.
 *
 * @since 15.0
 * @author José Bolina
 */
public final class Resp3Response {

   private Resp3Response() { }

   public static final BiConsumer<Object, ByteBufPool> OK = (ignore, alloc) -> ok(alloc);
   public static final BiConsumer<CharSequence, ByteBufPool> SIMPLE_STRING = Resp3Response::simpleString;
   public static final BiConsumer<byte[], ByteBufPool> BULK_STRING_BYTES = Resp3Response::string;
   public static final BiConsumer<CharSequence, ByteBufPool> BULK_STRING = Resp3Response::string;
   public static final BiConsumer<Number, ByteBufPool> INTEGER = Resp3Response::integers;
   public static final BiConsumer<Number, ByteBufPool> DOUBLE = Resp3Response::doubles;
   public static final BiConsumer<Object, ByteBufPool> UNKNOWN = Resp3Response::serialize;
   public static final BiConsumer<JavaObjectSerializer<?>, ByteBufPool> CUSTOM = (res, alloc) -> Resp3Response.write(res, alloc, (JavaObjectSerializer<Object>) res);

   /**
    * List the consumers for array responses with the different types needed.
    * Add new types as necessary.
    */
   public static final BiConsumer<Collection<byte[]>, ByteBufPool> ARRAY_BULK_STRING = (c, alloc) -> Resp3Response.array(c, alloc, Resp3Type.BULK_STRING);
   public static final BiConsumer<Collection<? extends Number>, ByteBufPool> ARRAY_INTEGER = (c, alloc) -> Resp3Response.array(c, alloc, Resp3Type.INTEGER);
   public static final BiConsumer<Collection<? extends Number>, ByteBufPool> ARRAY_DOUBLE = (c, alloc) -> Resp3Response.array(c, alloc, Resp3Type.DOUBLE);

   /**
    * List the consumers for set responses with the different types needed.
    */
   public static final BiConsumer<Set<byte[]>, ByteBufPool> SET_BULK_STRING = (s, alloc) -> Resp3Response.set(s, alloc, Resp3Type.BULK_STRING);

   /**
    * List the consumers for map responses with the different types needed.
    */
   public static final BiConsumer<Map<byte[], byte[]>, ByteBufPool> MAP_BULK_STRING_KV = (m, a) -> Resp3Response.map(m, a, Resp3Type.BULK_STRING);

   /**
    * Writes the null value.
    *
    * @param alloc Buffer pool for allocation.
    */
   public static void nulls(ByteBufPool alloc) {
      PrimitiveSerializer.NullSerializer.INSTANCE.accept(null, alloc);
   }

   /**
    * Serializes a simple string with the <code>"OK"</code> content.
    *
    * @param alloc: Buffer pool for allocation.
    */
   public static void ok(ByteBufPool alloc) {
      simpleString(RespConstants.OK, alloc);
   }

   /**
    * Serializes a simple string with the <code>"QUEUED"</code> content.
    *
    * @param ignore Content to ignore.
    * @param alloc Buffer pool for allocation.
    */
   public static void queued(Object ignore, ByteBufPool alloc) {
      simpleString(RespConstants.QUEUED_REPLY, alloc);
   }

   /**
    * Serializes a char sequence in a simple string format.
    *
    * @param value The ASCII string to serialize.
    * @param alloc Buffer pool for allocation.
    * @see PrimitiveSerializer.SimpleStringSerializer
    */
   public static void simpleString(CharSequence value, ByteBufPool alloc) {
      serialize(value, alloc, PrimitiveSerializer.SimpleStringSerializer.INSTANCE);
   }

   /**
    * Serializes a char sequence in a bulk string format.
    *
    * @param value The string to serialize.
    * @param alloc Buffer pool for allocation.
    * @see PrimitiveSerializer.BulkStringSerializer2
    */
   public static void string(CharSequence value, ByteBufPool alloc) {
      serialize(value, alloc, PrimitiveSerializer.BulkStringSerializer2.INSTANCE);
   }

   /**
    * Serializes the binary blob in a bulk string format.
    *
    * @param value The binary blob to serialize.
    * @param alloc Buffer pool for allocation.
    * @see PrimitiveSerializer.BulkStringSerializer
    */
   public static void string(byte[] value, ByteBufPool alloc) {
      serialize(value, alloc, PrimitiveSerializer.BulkStringSerializer.INSTANCE);
   }

   /**
    * Serializes a 64-bit number in the integer format.
    *
    * @param value Number to serialize.
    * @param alloc Buffer pool for allocation.
    * @see PrimitiveSerializer.IntegerSerializer
    */
   public static void integers(Number value, ByteBufPool alloc) {
      serialize(value, alloc, PrimitiveSerializer.IntegerSerializer.INSTANCE);
   }

   /**
    * Serializes a double-precision floating point into the doubles format.
    *
    * @param value The floating point to serialize.
    * @param alloc Buffer pool for allocation.
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see DoubleSerializer
    */
   public static void doubles(Number value, ByteBufPool alloc) {
      serialize(value, alloc, DoubleSerializer.INSTANCE);
   }

   /**
    * Serializes a boolean value in the RESP3 format.
    *
    * @param value Boolean value to serialize.
    * @param alloc Buffer pool for allocation.
    * @see PrimitiveSerializer.BooleanSerializer
    */
   public static void booleans(boolean value, ByteBufPool alloc) {
      serialize(value, alloc, PrimitiveSerializer.BooleanSerializer.INSTANCE);
   }

   /**
    * Serializes a collection in the array format.
    *
    * @param alloc Buffer pool for allocation.
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see CollectionSerializer.ArraySerializer
    */
   public static void arrayEmpty(ByteBufPool alloc) {
      write(Collections.emptyList(), alloc, (ignore, a) -> ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, 0, a));
   }

   /**
    * Serializes the collection by delegating the serialization of the elements to the provided serializer.
    *
    * @param collection Collection of elements to serialize.
    * @param alloc Buffer pool for allocation.
    * @param serializer Serializer for a single element in the collection.
    * @param <T> The type of the elements in the collection and the serializer handles.
    * @see CollectionSerializer.ArraySerializer
    */
   @SuppressWarnings("unchecked")
   public static <T> void array(Collection<T> collection, ByteBufPool alloc, JavaObjectSerializer<T> serializer) {
      serialize(collection, alloc, CollectionSerializer.ArraySerializer.INSTANCE, (o, b) -> serializer.accept((T) o, b));
   }

   /**
    * Serializes a collection in the array format with elements of a specified type.
    *
    * @param collection Collection of heterogeneous values to serialize.
    * @param alloc Buffer pool for allocation.
    * @param contentType The type of elements contained by the sequence.
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see CollectionSerializer.ArraySerializer
    */
   public static void array(Collection<?> collection, ByteBufPool alloc, Resp3Type contentType) {
      serialize(collection, alloc, CollectionSerializer.ArraySerializer.INSTANCE, contentType);
   }

   /**
    * Serializes a set in the RESP3 set format.
    *
    * @param alloc Buffer pool for allocation.
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see CollectionSerializer.SetSerializer
    */
   public static void emptySet(ByteBufPool alloc) {
      write(Collections.emptySet(), alloc, (ignore, a) -> ByteBufferUtils.writeNumericPrefix(RespConstants.SET, 0, a));
   }

   /**
    * Serializes a set in the RESP3 set format with elements of a specified type.
    *
    * @param set Set of heterogeneous values to serialize.
    * @param alloc Buffer pool for allocation.
    * @param contentType The type of elements contained by the set.
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see CollectionSerializer.SetSerializer
    */
   public static void set(Set<?> set, ByteBufPool alloc, Resp3Type contentType) {
      serialize(set, alloc, CollectionSerializer.SetSerializer.INSTANCE, contentType);
   }

   /**
    * Serializes a map in the RESP3 map format.
    *
    * @param value A map with heterogeneous key-value tuples to serialize.
    * @param alloc Buffer pool for allocation.
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see MapSerializer
    */
   public static void map(Map<?, ?> value, ByteBufPool alloc) {
      serialize(value, alloc, MapSerializer.INSTANCE);
   }

   /**
    * Serializes a map in the RESP3 map format with key and values of the same specified type.
    *
    * @param value A map with heterogeneous key-value tuples to serialize.
    * @param alloc Buffer pool for allocation.
    * @param contentType The type of key and value elements.
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see MapSerializer
    */
   public static void map(Map<?, ?> value, ByteBufPool alloc, Resp3Type contentType) {
      serialize(value, alloc, MapSerializer.INSTANCE, new SerializationHint.KeyValueHint(contentType, contentType));
   }

   /**
    * Serializes a map in the RESP3 map format with keys and values of a specified format.
    *
    * @param value A map with heterogeneous key-value tuples to serialize.
    * @param alloc Buffer pool for allocation.
    * @param keyType The type of keys in the map.
    * @param valueType The type of values in the map.
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see MapSerializer
    */
   public static void map(Map<?, ?> value, ByteBufPool alloc, Resp3Type keyType, Resp3Type valueType) {
      serialize(value, alloc, MapSerializer.INSTANCE, new SerializationHint.KeyValueHint(keyType, valueType));
   }

   /**
    * Serializes an error message in the RESP3 format.
    *
    * <p>
    * The first character in the error message <b>must</b> be the <code>'-'</code> symbol.
    * </p>
    *
    * @param value An ASCII char sequence with the error message.
    * @param alloc Buffer pool for allocation.
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see PrimitiveSerializer.SimpleErrorSerializer
    */
   public static void error(CharSequence value, ByteBufPool alloc) {
      serialize(value, alloc, PrimitiveSerializer.SimpleErrorSerializer.INSTANCE);
   }

   /**
    * Serializes the exception message in the RESP3 error format.
    *
    * @param t The throwable to serialize.
    * @param alloc Buffer pool for allocation
    * @see ThrowableSerializer
    */
   public static void error(Throwable t, ByteBufPool alloc) {
      serialize(t, alloc, ThrowableSerializer.INSTANCE);
   }

   /**
    * Writes an object utilizing the specific serializer.
    * <p>
    * Implementors do not need to check for nullability in the implementation. Null response values are handled
    * by native RESP3 serializers before passing the serialization ahead.
    * </p>
    *
    * @param object The element to serialize.
    * @param alloc Buffer pool for allocation.
    * @param serializer The serializer to utilize.
    * @param <T> The type of the object.
    */
   public static <T> void write(T object, ByteBufPool alloc, JavaObjectSerializer<T> serializer) {
      serialize(object, alloc, serializer);
   }

   /**
    * Invokes the callback to write a response with the serializer.
    *
    * @param alloc Buffer pool for allocation.
    * @param serializer Serializer to write.
    */
   public static <T> void write(ByteBufPool alloc, JavaObjectSerializer<T> serializer) {
      serialize(alloc, alloc, serializer);
   }

   /**
    * Generic method to serialize an object of unknown type.
    *
    * <p>
    * This method searches all serializers available in the registry.
    * </p>
    *
    * @param object The object to serialize in RESP3 format.
    * @param alloc Buffer pool for allocation.
    * @throws IllegalStateException in case no serializer is found for the object.
    */
   public static void serialize(Object object, ByteBufPool alloc) {
      Resp3SerializerRegistry.serialize(object, alloc);
   }

   /**
    * Generic method to serialize an object of unknown type.
    *
    * <p>
    * This method is restricted to the provided candidate serializer and a null check.
    * </p>
    *
    * @param object The object to serialize in RESP3 format.
    * @param alloc Buffer pool for allocation.
    * @param candidate The candidate serializer in case the object is non-null.
    * @throws IllegalStateException in case no serializer is found for the object.
    */
   private static void serialize(Object object, ByteBufPool alloc, ResponseSerializer<?> candidate) {
      Resp3SerializerRegistry.serialize(object, alloc, candidate);
   }

   private static <H extends SerializationHint> void serialize(Object object, ByteBufPool alloc,
                                                               NestedResponseSerializer<?, H> candidate, H hint) {
      Resp3SerializerRegistry.serialize(object, alloc, candidate, hint);
   }
}
