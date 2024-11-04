package org.infinispan.server.resp.serialization.bytebuf;

import static org.infinispan.server.resp.serialization.RespConstants.CRLF;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.NestedResponseSerializer;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.server.resp.serialization.ResponseSerializer;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.server.resp.serialization.SerializationHint;

import io.netty.buffer.ByteBuf;

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
 * <p>
 * The method throws an exception when an object does not have a serializer.
 *
 * @author José Bolina
 * @since 15.0
 */
public final class ByteBufResponseWriter implements ResponseWriter {
   private final ByteBufPool alloc;

   public ByteBufResponseWriter(ByteBufPool alloc) {
      this.alloc = alloc;
   }

   /**
    * Writes the null value.
    */
   @Override
   public void nulls() {
      ByteBufPrimitiveSerializer.NullSerializer.INSTANCE.accept(null, alloc);
   }

   /**
    * Serializes a simple string with the <code>"OK"</code> content.
    */
   @Override
   public void ok() {
      simpleString(RespConstants.OK);
   }

   /**
    * Serializes a simple string with the <code>"QUEUED"</code> content.
    *
    * @param ignore Content to ignore.
    */
   @Override
   public void queued(Object ignore) {
      simpleString(RespConstants.QUEUED_REPLY);
   }

   /**
    * Serializes a char sequence in a simple string format.
    *
    * @param value The ASCII string to serialize.
    * @see ByteBufPrimitiveSerializer.SimpleStringSerializer
    */
   @Override
   public void simpleString(CharSequence value) {
      serialize(value, ByteBufPrimitiveSerializer.SimpleStringSerializer.INSTANCE);
   }

   /**
    * Serializes a char sequence in a bulk string format.
    *
    * @param value The string to serialize.
    * @see ByteBufPrimitiveSerializer.BulkStringSerializer2
    */
   @Override
   public void string(CharSequence value) {
      serialize(value, ByteBufPrimitiveSerializer.BulkStringSerializer2.INSTANCE);
   }

   /**
    * Serializes the binary blob in a bulk string format.
    *
    * @param value The binary blob to serialize.
    * @see ByteBufPrimitiveSerializer.BulkStringSerializer
    */
   @Override
   public void string(byte[] value) {
      serialize(value, ByteBufPrimitiveSerializer.BulkStringSerializer.INSTANCE);
   }

   /**
    * Serializes a 64-bit number in the integer format.
    *
    * @param value Number to serialize.
    * @see ByteBufPrimitiveSerializer.IntegerSerializer
    */
   @Override
   public void integers(Number value) {
      serialize(value, ByteBufPrimitiveSerializer.IntegerSerializer.INSTANCE);
   }

   /**
    * Serializes a double-precision floating point into the doubles format.
    *
    * @param value The floating point to serialize.
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see ByteBufDoubleSerializer
    */
   @Override
   public void doubles(Number value) {
      serialize(value, ByteBufDoubleSerializer.INSTANCE);
   }

   /**
    * Serializes a boolean value in the RESP3 format.
    *
    * @param value Boolean value to serialize.
    * @see ByteBufPrimitiveSerializer.BooleanSerializer
    */
   @Override
   public void booleans(boolean value) {
      serialize(value, ByteBufPrimitiveSerializer.BooleanSerializer.INSTANCE);
   }

   /**
    * Serializes a collection in the array format.
    *
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see ByteBufCollectionSerializer.ArraySerializer
    */
   @Override
   public void arrayEmpty() {
      write(Collections.emptyList(), (ignore, writer) -> writer.writeNumericPrefix(RespConstants.ARRAY, 0));
   }

   /**
    * Serializes the collection by delegating the serialization of the elements to the provided serializer.
    *
    * @param collection Collection of elements to serialize.
    * @param serializer Serializer for a single element in the collection.
    * @param <T>        The type of the elements in the collection and the serializer handles.
    * @see ByteBufCollectionSerializer.ArraySerializer
    */
   @SuppressWarnings("unchecked")
   @Override
   public <T> void array(Collection<T> collection, JavaObjectSerializer<T> serializer) {
      serialize(collection, ByteBufCollectionSerializer.ArraySerializer.INSTANCE, (o, b) -> serializer.accept((T) o, b));
   }

   /**
    * Serializes a collection in the array format with elements of a specified type.
    *
    * @param collection  Collection of heterogeneous values to serialize.
    * @param contentType The type of elements contained by the sequence.
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see ByteBufCollectionSerializer.ArraySerializer
    */
   @Override
   public void array(Collection<?> collection, Resp3Type contentType) {
      serialize(collection, ByteBufCollectionSerializer.ArraySerializer.INSTANCE, contentType);
   }

   /**
    * Serializes a set in the RESP3 set format.
    *
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see ByteBufCollectionSerializer.SetSerializer
    */
   @Override
   public void emptySet() {
      write(Collections.emptySet(), (ignore, writer) -> writer.writeNumericPrefix(RespConstants.SET, 0));
   }

   /**
    * Serializes a set in the RESP3 set format with elements of a specified type.
    *
    * @param set         Set of heterogeneous values to serialize.
    * @param contentType The type of elements contained by the set.
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see ByteBufCollectionSerializer.SetSerializer
    */
   @Override
   public void set(Set<?> set, Resp3Type contentType) {
      serialize(set, ByteBufCollectionSerializer.SetSerializer.INSTANCE, contentType);
   }

   @Override
   public <T> void set(Set<?> set, JavaObjectSerializer<T> serializer) {
      serialize(set, ByteBufCollectionSerializer.SetSerializer.INSTANCE, (o, b) -> serializer.accept((T) o, b));
   }

   /**
    * Serializes a map in the RESP3 map format.
    *
    * @param value A map with heterogeneous key-value tuples to serialize.
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see ByteBufMapSerializer
    */
   @Override
   public void map(Map<?, ?> value) {
      serialize(value, ByteBufMapSerializer.INSTANCE);
   }

   /**
    * Serializes a map in the RESP3 map format with key and values of the same specified type.
    *
    * @param value       A map with heterogeneous key-value tuples to serialize.
    * @param contentType The type of key and value elements.
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see ByteBufMapSerializer
    */
   @Override
   public void map(Map<?, ?> value, Resp3Type contentType) {
      serialize(value, ByteBufMapSerializer.INSTANCE, new SerializationHint.KeyValueHint(contentType, contentType));
   }

   /**
    * Serializes a map in the RESP3 map format with keys and values of a specified format.
    *
    * @param value     A map with heterogeneous key-value tuples to serialize.
    * @param keyType   The type of keys in the map.
    * @param valueType The type of values in the map.
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see ByteBufMapSerializer
    */
   @Override
   public void map(Map<?, ?> value, Resp3Type keyType, Resp3Type valueType) {
      serialize(value, ByteBufMapSerializer.INSTANCE, new SerializationHint.KeyValueHint(keyType, valueType));
   }

   @Override
   public <T> void map(Map<?, ?> map, JavaObjectSerializer<T> serializer) {
      serialize(map, ByteBufMapSerializer.INSTANCE, new SerializationHint.KeyValueHint(null, null));
   }

   /**
    * Serializes an error message in the RESP3 format.
    *
    * <p>
    * The first character in the error message <b>must</b> be the <code>'-'</code> symbol.
    * </p>
    *
    * @param value An ASCII char sequence with the error message.
    * @throws IllegalStateException in case no serializer is found for the object.
    * @see ByteBufPrimitiveSerializer.SimpleErrorSerializer
    */
   @Override
   public void error(CharSequence value) {
      serialize(value, ByteBufPrimitiveSerializer.SimpleErrorSerializer.INSTANCE);
   }

   /**
    * Serializes the exception message in the RESP3 error format.
    *
    * @param t The throwable to serialize.
    * @see ByteBufThrowableSerializer
    */
   @Override
   public void error(Throwable t) {
      serialize(t, ByteBufThrowableSerializer.INSTANCE);
   }

   /**
    * Writes an object utilizing the specific serializer.
    * <p>
    * Implementors do not need to check for nullability in the implementation. Null response values are handled
    * by native RESP3 serializers before passing the serialization ahead.
    * </p>
    *
    * @param object     The element to serialize.
    * @param serializer The serializer to utilize.
    * @param <T>        The type of the object.
    */
   @Override
   public <T> void write(T object, JavaObjectSerializer<T> serializer) {
      if (object == null) {
         nulls();
      } else {
         serializer.accept(object, this);
      }
   }

   /**
    * Invokes the callback to write a response with the serializer.
    *
    * @param serializer Serializer to write.
    */
   @Override
   public <T> void write(JavaObjectSerializer<T> serializer) {
      if (serializer == null) {
         nulls();
      } else {
         serializer.accept(null, this);
      }
   }

   /**
    * Generic method to serialize an object of unknown type.
    *
    * <p>
    * This method searches all serializers available in the registry.
    * </p>
    *
    * @param object The object to serialize in RESP3 format.
    * @throws IllegalStateException in case no serializer is found for the object.
    */
   @Override
   public void serialize(Object object) {
      ByteBufSerializerRegistry.serialize(object, alloc);
   }

   @Override
   public void writeNumericPrefix(byte symbol, long number, int additionalWidth) {
      int decimalWidth = ByteBufferUtils.stringSize(number);
      int size = 1 + decimalWidth + 2 + additionalWidth;
      ByteBuf buffer = alloc.acquire(size);
      buffer.writeByte(symbol);
      ByteBufferUtils.setIntChars(number, decimalWidth, buffer);
      buffer.writeBytes(CRLF);
   }

   /**
    * Generic method to serialize an object of unknown type.
    *
    * <p>
    * This method is restricted to the provided candidate serializer and a null check.
    * </p>
    *
    * @param object    The object to serialize in RESP3 format.
    * @param candidate The candidate serializer in case the object is non-null.
    * @throws IllegalStateException in case no serializer is found for the object.
    */
   private void serialize(Object object, ResponseSerializer<?, ByteBufPool> candidate) {
      ByteBufSerializerRegistry.serialize(object, alloc, candidate);
   }

   private <H extends SerializationHint> void serialize(Object object,
                                                        NestedResponseSerializer<?, ByteBufPool, H> candidate,
                                                        H hint) {
      ByteBufSerializerRegistry.serialize(object, alloc, candidate, hint);
   }
}
