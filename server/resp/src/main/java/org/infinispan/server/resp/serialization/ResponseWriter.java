package org.infinispan.server.resp.serialization;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.security.sasl.SaslException;

import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespVersion;
import org.infinispan.server.resp.exception.RespCommandException;

/**
 * Entrypoint to serialize a response in RESP format.
 *
 * <p>
 * The class provides specific methods for the RESP types. In cases where the response object type is unknown or
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
public interface ResponseWriter {
   BiConsumer<Object, ResponseWriter> OK = (ignore, writer) -> writer.ok();
   BiConsumer<CharSequence, ResponseWriter> SIMPLE_STRING = (s, writer) -> writer.simpleString(s);
   BiConsumer<byte[], ResponseWriter> BULK_STRING_BYTES = (b, writer) -> writer.string(b);
   BiConsumer<CharSequence, ResponseWriter> BULK_STRING = (s, writer) -> writer.string(s);
   BiConsumer<Number, ResponseWriter> INTEGER = (i, writer) -> writer.integers(i);
   BiConsumer<Number, ResponseWriter> DOUBLE = (d, writer) -> writer.doubles(d);
   BiConsumer<Object, ResponseWriter> UNKNOWN = (o, writer) -> writer.serialize(o);
   BiConsumer<JavaObjectSerializer<?>, ResponseWriter> CUSTOM = (res, writer) -> writer.write((JavaObjectSerializer<Object>) res);

   /**
    * List the consumers for array responses with the different types needed.
    * Add new types as necessary.
    */
   BiConsumer<Collection<byte[]>, ResponseWriter> ARRAY_BULK_STRING = (c, writer) -> writer.array(c, Resp3Type.BULK_STRING);
   BiConsumer<Collection<? extends Number>, ResponseWriter> ARRAY_INTEGER = (c, writer) -> writer.array(c, Resp3Type.INTEGER);
   BiConsumer<Collection<? extends Number>, ResponseWriter> ARRAY_DOUBLE = (c, writer) -> writer.array(c, Resp3Type.DOUBLE);
   BiConsumer<Collection<? extends String>, ResponseWriter> ARRAY_STRING = (c, writer) -> writer.array(c, Resp3Type.BULK_STRING);

   /**
    * List the consumers for set responses with the different types needed.
    */
   BiConsumer<Set<byte[]>, ResponseWriter> SET_BULK_STRING = (s, writer) -> writer.set(s, Resp3Type.BULK_STRING);

   /**
    * List the consumers for map responses with the different types needed.
    */
   BiConsumer<Map<byte[], byte[]>, ResponseWriter> MAP_BULK_STRING_KV = (m, writer) -> writer.map(m, Resp3Type.BULK_STRING);

   boolean isInternal();

   RespVersion version();

   void version(RespVersion version);

   /**
    * Writes the null value.
    */
   void nulls();

   /**
    * Serializes a simple string with the <code>"OK"</code> content.
    */
   void ok();

   /**
    * Serializes a simple string with the <code>"QUEUED"</code> content.
    *
    * @param ignore Content to ignore.
    */
   void queued(Object ignore);

   /**
    * Serializes a char sequence in a simple string format.
    *
    * @param value The ASCII string to serialize.
    */
   void simpleString(CharSequence value);

   /**
    * Serializes a char sequence in a bulk string format.
    *
    * @param value The string to serialize.
    */
   void string(CharSequence value);

   /**
    * Serializes the binary blob in a bulk string format.
    *
    * @param value The binary blob to serialize.
    */
   void string(byte[] value);

   /**
    * Serializes a 64-bit number in the integer format.
    *
    * @param value Number to serialize.
    */
   void integers(Number value);

   /**
    * Serializes a double-precision floating point into the doubles format.
    *
    * @param value The floating point to serialize.
    * @throws IllegalStateException in case no serializer is found for the object.
    */
   void doubles(Number value);

   /**
    * Serializes a boolean value in the RESP3 format.
    *
    * @param value Boolean value to serialize.
    */
   void booleans(boolean value);

   /**
    * Serializes a collection in the array format.
    *
    * @throws IllegalStateException in case no serializer is found for the object.
    */
   void arrayEmpty();

   /**
    * Serializes the collection by delegating the serialization of the elements to the provided serializer.
    *
    * @param collection Collection of elements to serialize.
    * @param serializer Serializer for a single element in the collection.
    * @param <T>        The type of the elements in the collection and the serializer handles.
    */
   <T> void array(Collection<T> collection, JavaObjectSerializer<T> serializer);

   /**
    * Serializes a collection in the array format with elements of a specified type.
    *
    * @param collection  Collection of heterogeneous values to serialize.
    * @param contentType The type of elements contained by the sequence.
    * @throws IllegalStateException in case no serializer is found for the object.
    */
   void array(Collection<?> collection, Resp3Type contentType);


   /**
    * Serializes a set
    *
    * @throws IllegalStateException in case no serializer is found for the object.
    */
   void emptySet();


   /**
    * Serializes a set with elements of a specified type.
    *
    * @param set         Set of heterogeneous values to serialize.
    * @param contentType The type of elements contained by the set.
    * @throws IllegalStateException in case no serializer is found for the object.
    */
   void set(Set<?> set, Resp3Type contentType);

   <T> void set(Set<?> set, JavaObjectSerializer<T> serializer);

   /**
    * Serializes a map.
    *
    * @param value A map with heterogeneous key-value tuples to serialize.
    * @throws IllegalStateException in case no serializer is found for the object.
    */
   void map(Map<?, ?> value);

   /**
    * Serializes a map with key and values of the same specified type.
    *
    * @param value       A map with heterogeneous key-value tuples to serialize.
    * @param contentType The type of key and value elements.
    * @throws IllegalStateException in case no serializer is found for the object.
    */
   void map(Map<?, ?> value, Resp3Type contentType);

   /**
    * Serializes a map in the RESP3 map format with keys and values of a specified format.
    *
    * @param value     A map with heterogeneous key-value tuples to serialize.
    * @param keyType   The type of keys in the map.
    * @param valueType The type of values in the map.
    * @throws IllegalStateException in case no serializer is found for the object.
    */
   void map(Map<?, ?> value, Resp3Type keyType, Resp3Type valueType);

   void map(Map<?, ?> map, SerializationHint.KeyValueHint keyValueHint);

   /**
    * Serializes an error message.
    *
    * <p>
    * The first character in the error message <b>must</b> be the <code>'-'</code> symbol.
    * </p>
    *
    * @param value An ASCII char sequence with the error message.
    * @throws IllegalStateException in case no serializer is found for the object.
    */
   void error(CharSequence value);

   /**
    * Serializes the exception message in the RESP3 error format.
    *
    * @param t The throwable to serialize.
    */
   void error(Throwable t);

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
   <T> void write(T object, JavaObjectSerializer<T> serializer);

   /**
    * Invokes the callback to write a response with the serializer.
    *
    * @param serializer Serializer to write.
    */
   <T> void write(JavaObjectSerializer<T> serializer);

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
   void serialize(Object object);

   void writeNumericPrefix(byte symbol, long number);

   default void syntaxError() {
      error("-ERR syntax error");
   }

   default void unknownCommand() {
      error("-ERR unknown command");
   }

   default void unauthorized() {
      error("-WRONGPASS invalid username-password pair or user is disabled.");
   }

   default void customError(String error) {
      error("-ERR " + error);
   }

   default void wrongArgumentCount(RespCommand command) {
      error("ERR wrong number of arguments for '" + command.getName().toLowerCase() + "' command");
   }

   default void nanOrInfinity() {
      error("-ERR increment would produce NaN or Infinity");
   }

   default void valueNotInteger() {
      error("-ERR value is not an integer or out of range");
   }

   default void valueNotAValidFloat() {
      error("-ERR value is not a valid float");
   }

   default void minOrMaxNotAValidFloat() {
      error("-ERR min or max is not a float");
   }

   default void minOrMaxNotAValidStringRange() {
      error("-ERR min or max not valid string range item");
   }

   default void transactionAborted() {
      error("-EXECABORT Transaction discarded because of previous errors.");
   }

   default void mustBePositive(String argumentName) {
      error("-ERR value for ' " + argumentName + "' is out of range, must be positive");
   }

   default void mustBePositive() {
      error("-ERR value is out of range, must be positive");
   }

   default void wrongArgumentNumber(RespCommand command) {
      error("-ERR wrong number of arguments for '" + command.getName().toLowerCase() + "' command");
   }

   default void indexOutOfRange() {
      error("-ERR index out of range");
   }

   default void noSuchKey() {
      error("-ERR no such key");
   }

   default void wrongType() {
      error("-WRONGTYPE Operation against a key holding the wrong kind of value");
   }

   static Consumer<ResponseWriter> handleException(Throwable t) {
      for (Throwable ex = t; ex != null; ex = ex.getCause()) {
         if (ex instanceof RespCommandException rce) {
            return rw -> rw.error(String.format("-%s", rce.getMessage()));
         }

         if (ex instanceof ClassCastException) {
            return ResponseWriter::wrongType;
         }

         if (ex instanceof IllegalArgumentException) {
            if (ex.getMessage().contains("No marshaller registered for object of Java type")) {
               return ResponseWriter::wrongType;
            }
         }

         if (ex instanceof IndexOutOfBoundsException) {
            return ResponseWriter::indexOutOfRange;
         }

         if (ex instanceof NumberFormatException) {
            return ResponseWriter::valueNotInteger;
         }

         if (ex instanceof SecurityException || ex instanceof SaslException) {
            return ResponseWriter::unauthorized;
         }
      }

      return null;
   }

   /**
    * Start writing an array
    * @param size the number of elements
    */
   void arrayStart(int size);

   /**
    * Finish writing an array
    */
   void arrayEnd();

   /**
    * Start writing a new array element
    */
   void arrayNext();
}
