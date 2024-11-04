package org.infinispan.server.resp.serialization;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.infinispan.commons.CacheException;
import org.infinispan.server.resp.RespCommand;

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

   /**
    * List the consumers for set responses with the different types needed.
    */
   BiConsumer<Set<byte[]>, ResponseWriter> SET_BULK_STRING = (s, writer) -> writer.set(s, Resp3Type.BULK_STRING);

   /**
    * List the consumers for map responses with the different types needed.
    */
   BiConsumer<Map<byte[], byte[]>, ResponseWriter> MAP_BULK_STRING_KV = (m, writer) -> writer.map(m, Resp3Type.BULK_STRING);

   void nulls();

   void ok();

   void queued(Object ignore);

   void simpleString(CharSequence value);

   void string(CharSequence value);

   void string(byte[] value);

   void integers(Number value);

   void doubles(Number value);

   void booleans(boolean value);

   void arrayEmpty();

   <T> void array(Collection<T> collection, JavaObjectSerializer<T> serializer);

   void array(Collection<?> collection, Resp3Type contentType);

   void emptySet();

   void set(Set<?> set, Resp3Type contentType);

   <T> void set(Set<?> set, JavaObjectSerializer<T> serializer);

   void map(Map<?, ?> value);

   void map(Map<?, ?> value, Resp3Type contentType);

   void map(Map<?, ?> value, Resp3Type keyType, Resp3Type valueType);

   <T> void map(Map<?, ?> map, JavaObjectSerializer<T> serializer);

   void error(CharSequence value);

   void error(Throwable t);

   <T> void write(T object, JavaObjectSerializer<T> serializer);

   <T> void write(JavaObjectSerializer<T> serializer);

   void serialize(Object object);

   default void writeNumericPrefix(byte symbol, long number) {
      writeNumericPrefix(symbol, number, 0);
   }

   void writeNumericPrefix(byte symbol, long number, int additionalWidth);

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
      Throwable ex = t;
      while (ex instanceof CompletionException || ex instanceof CacheException) {
         ex = ex.getCause();
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

      return null;
   }
}
