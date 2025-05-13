package org.infinispan.server.resp.serialization.bytebuf;

import static org.infinispan.server.resp.serialization.RespConstants.CRLF;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;

import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.server.resp.serialization.ResponseSerializer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

/**
 * REdis Serialization Protocol (RESP) for primitive values in version 3.
 * <p>
 * RESP is the wire protocol for communication between client and server. The protocol is text-based and utilizes
 * prefixes/special characters to segment messages. We strictly follow the RESP3 serialization format for all objects.
 * RESP3 is a superset of RESP2, providing interoperation with objects in version 2 and adding more elements and signals in version 3.
 * </p>
 *
 * <p>
 * The basic idea of the protocol is to prefix an element with a magic byte to identify the type. The {@link RespConstants#CRLF_STRING}
 * is the protocol terminator that segments the elements. Varying-sized elements have a numeric prefix to determine the size.
 * RESP protocol serializes numbers in base 10. Next, we cover each primitive type.
 * </p>
 *
 * <p>
 * In this class, we cover the serialization of primitive types in RESP3. The messages are composed of primitive or
 * aggregate types, such as arrays and hashes. Aggregate types are composed of other aggregate types or primitives.
 * </p>
 *
 * @see <a href="https://redis.io/docs/latest/develop/reference/protocol-spec/">RESP Specification</a>
 * @see <a href="https://github.com/antirez/RESP3/blob/74adea588783e463c7e84793b325b088fe6edd1c/spec.md/">RESP3 Specification</a>
 * @since 15.0
 * @author José Bolina
 */
class ByteBufPrimitiveSerializer {
   /**
    * Group the serializers for bulk string.
    */
   static ResponseSerializer<?, ?>[] BULK_STRING_SERIALIZERS = {
     BulkStringSerializer.INSTANCE,
     BulkStringSerializer2.INSTANCE,
   };
   /**
    * Group all serializers, except the null.
    */
   static Collection<ResponseSerializer<?, ByteBufPool>> SERIALIZERS = List.of(
         SimpleStringSerializer.INSTANCE,
         BulkStringSerializer.INSTANCE,
         BulkStringSerializer2.INSTANCE,
         IntegerSerializer.INSTANCE,
         SimpleErrorSerializer.INSTANCE,
         BooleanSerializer.INSTANCE
   );

   /**
    * Represent non-existent values.
    *
    * <p>
    * The prefix is the underscore character ({@link RespConstants#NULL}), followed by the separator.
    * </p>
    *
    * <b>Warning:</b> Always check with the null serializer first before trying any other. This approach guarantees the
    * value is non-null during the serialization.
    */
   static final class NullSerializer implements ResponseSerializer<Object, ByteBufPool> {
      public static final NullSerializer INSTANCE = new NullSerializer();

      @Override
      public void accept(Object object, ByteBufPool alloc) {
         assert object == null : "Only handle null values";

         // RESP: _\r\n
         alloc.acquire(1 + CRLF.length)
               .writeByte(RespConstants.NULL)
               .writeBytes(CRLF);
      }

      @Override
      public boolean test(Object object) {
         return object == null;
      }
   }

   /**
    * Represent short, non-binary strings.
    *
    * <p>
    * The prefix is the plus character ({@link RespConstants#SIMPLE_STRING}), followed by the actual string, and the suffix
    * is the terminator symbol. The string contents should not contain either <code>'\r'</code> or <code>'\n'</code> characters.
    * </p>
    *
    * <p>
    * Observe that, in our implementation, the simple string overlaps with the bulk strings. Elements required to be
    * bulk strings must be byte arrays.
    * </p>
    */
   static final class SimpleStringSerializer implements ResponseSerializer<CharSequence, ByteBufPool> {
      public static final SimpleStringSerializer INSTANCE = new SimpleStringSerializer();

      @Override
      public void accept(CharSequence charSequence, ByteBufPool alloc) {
         // The sequence is guaranteed to contain only ASCII characters now.
         int contentLength = charSequence.length();
         int size = 1 + contentLength + CRLF.length;

         // RESP: +<data>\r\n
         ByteBuf buffer = alloc.acquire(size).writeByte(RespConstants.SIMPLE_STRING);
         for (int i = 0; i < charSequence.length(); i++) {
            buffer.writeByte(charSequence.charAt(i));
         }
         buffer.writeBytes(CRLF);
      }

      @Override
      public boolean test(Object object) {
         if (!(object instanceof CharSequence cs)) return false;

         // Delegate to bulk string
         if (cs.isEmpty()) return false;

         // Is a simple error message.
         if (cs.charAt(0) == '-') return false;

         for (int i = 0; i < cs.length(); i++) {
            char c = cs.charAt(i);
            // Does not contain any of the separator symbol.
            if (c == CRLF[0] || cs.charAt(i) == CRLF[1])
               return false;

            // Contains only ASCII symbols.
            if ((c & 0x80) != 0)
               return false;
         }
         return true;
      }
   }

   /**
    * Represent an error message.
    *
    * <p>
    * It is the same as simple strings, but the prefix is the minus characters. We only check for char sequences because
    * we write errors with simple ASCII strings.
    * </p>
    *
    */
   static final class SimpleErrorSerializer implements ResponseSerializer<CharSequence, ByteBufPool> {
      public static final SimpleErrorSerializer INSTANCE = new SimpleErrorSerializer();

      @Override
      public void accept(CharSequence charSequence, ByteBufPool alloc) {
         // We only write errors in ASCII strings.
         int length = charSequence.length();
         // Adding extra space to escape \n char
         for (int i = 0; i < charSequence.length(); i++) {
            char charAt = charSequence.charAt(i);
            if ( charAt =='\n' || charAt=='\r') ++length;
         }
         int total = length + CRLF.length;
         // RESP: -<error message>\r\n
         ByteBuf buffer = alloc.acquire(total);
         for (int i = 0; i < charSequence.length(); i++) {
            char charAt = charSequence.charAt(i);
            // Escape char that break RESP
            switch (charAt) {
               case '\n':
                     buffer.writeByte('\\');
                     buffer.writeByte('n');
                  break;
               case '\r':
                  buffer.writeByte('\\');
                  buffer.writeByte('r');
               break;
               default:
                  buffer.writeByte(charAt);
            }
         }
         buffer.writeBytes(CRLF);
      }

      @Override
      public boolean test(Object object) {
         if (!(object instanceof CharSequence cs)) return false;
         return !cs.isEmpty() && cs.charAt(0) == '-';
      }
   }

   /**
    * Represent a signed, base-10, 64-bit integer.
    *
    * <p>
    * The prefix is the colon, followed by the base-10 representation, and the suffix with the terminator symbol. The base-10
    * representation might also contain a plus/minus symbol as the sign.
    * </p>
    */
   static final class IntegerSerializer implements ResponseSerializer<Number, ByteBufPool> {
      public static final IntegerSerializer INSTANCE = new IntegerSerializer();

      @Override
      public void accept(Number number, ByteBufPool alloc) {
         ByteBufferUtils.writeNumericPrefix(RespConstants.NUMERIC, number.longValue(), alloc);
      }

      @Override
      public boolean test(Object object) {
         // We cover any Number subclass (int, long up to AtomicInteger).
         if (object instanceof Number) {
            // Which is not floating point or big.
            return !(object instanceof Double
                  || object instanceof Float
                  || object instanceof BigDecimal
            );
         }
         return false;
      }
   }

   /**
    * Represent a single binary blob.
    *
    * <p>
    * A bulk string is a varying-size element that contains any characters, including the terminator symbols. Effectively,
    * it is a superset of {@link SimpleStringSerializer}. The prefix is the dollar sign ({@link RespConstants#BULK_STRING}),
    * with the base-10 number of the blob length and the terminator. Following is the actual blob data with the terminator
    * symbol as the suffix.
    * </p>
    */
   static final class BulkStringSerializer implements ResponseSerializer<byte[], ByteBufPool> {
      public static final BulkStringSerializer INSTANCE = new BulkStringSerializer();

      @Override
      public void accept(byte[] bytes, ByteBufPool alloc) {
         int contentLength = bytes.length;

         // RESP: $<length>\r\n<data>\r\n
         ByteBuf buffer = ByteBufferUtils.writeNumericPrefix(RespConstants.BULK_STRING, contentLength, alloc, contentLength + CRLF.length);
         buffer.writeBytes(bytes).writeBytes(CRLF);
      }

      @Override
      public boolean test(Object object) {
         // Handle any binary blob.
         return object instanceof byte[];
      }
   }

   /**
    * Bulk string for char sequences.
    *
    * @see BulkStringSerializer
    */
   static final class BulkStringSerializer2 implements ResponseSerializer<CharSequence, ByteBufPool> {
      public static final BulkStringSerializer2 INSTANCE = new BulkStringSerializer2();

      @Override
      public void accept(CharSequence charSequence, ByteBufPool alloc) {
         int contentLength = ByteBufUtil.utf8Bytes(charSequence);
         ByteBuf buffer = ByteBufferUtils.writeNumericPrefix(RespConstants.BULK_STRING, contentLength, alloc, contentLength + CRLF.length);
         ByteBufUtil.reserveAndWriteUtf8(buffer, charSequence, contentLength);
         buffer.writeBytes(CRLF);
      }

      @Override
      public boolean test(Object object) {
         if (!(object instanceof CharSequence cs)) return false;
         if (cs.isEmpty()) return true;

         // Handles any char sequence which is not an error message.
         return cs.charAt(0) != '-';
      }
   }

   /**
    * Represent boolean values.
    *
    * <p>
    * The prefix is the octothorpe symbol ({@link RespConstants#BOOLEAN}), followed by the character <code>'t'</code> for
    * true or <code>'f'</code> for false, and the suffix is the terminator symbol.
    * </p>
    */
   static final class BooleanSerializer implements ResponseSerializer<Boolean, ByteBufPool> {
      public static final BooleanSerializer INSTANCE = new BooleanSerializer();
      private static final byte TRUE = 't';
      private static final byte FALSE = 'f';

      @Override
      public void accept(Boolean b, ByteBufPool alloc) {
         int length = 1 + 1 + CRLF.length;

         // RESP: #<t|f>\r\n
         alloc.acquire(length)
               .writeByte(RespConstants.BOOLEAN)
               .writeByte(Boolean.TRUE.equals(b) ? TRUE : FALSE)
               .writeBytes(CRLF);
      }

      @Override
      public boolean test(Object object) {
         return object instanceof Boolean;
      }
   }


}
