package org.infinispan.server.memcached;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.util.KeyValuePair;

import io.netty.buffer.ByteBuf;

/**
 * Memcached text protocol utilities.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class TextProtocolUtil {
   private TextProtocolUtil() { }
   // todo: refactor name once old code has been removed?

   public static final String CRLF = "\r\n";
   public static final byte[] CRLFBytes = "\r\n".getBytes();
   public static final byte[] END = "END\r\n".getBytes();
   public static final int END_SIZE = END.length;
   public static final byte[] DELETED = "DELETED\r\n".getBytes();
   public static final byte[] NOT_FOUND = "NOT_FOUND\r\n".getBytes();
   public static final byte[] EXISTS = "EXISTS\r\n".getBytes();
   public static final byte[] STORED = "STORED\r\n".getBytes();
   public static final byte[] NOT_STORED = "NOT_STORED\r\n".getBytes();
   public static final byte[] OK = "OK\r\n".getBytes();
   public static final byte[] ERROR = "ERROR\r\n".getBytes();
   public static final String CLIENT_ERROR_BAD_FORMAT = "CLIENT_ERROR bad command line format: ";
   public static final String SERVER_ERROR = "SERVER_ERROR ";
   public static final byte[] VALUE = "VALUE ".getBytes();
   public static final int VALUE_SIZE = VALUE.length;
   public static final byte[] ZERO = "0".getBytes();

   public static final int SP = 32;
   public static final int CR = 13;
   public static final int LF = 10;

   public static final BigInteger MAX_UNSIGNED_LONG = new BigInteger("18446744073709551615");
   public static final BigInteger MIN_UNSIGNED = new BigInteger("0");

   public static final Charset CHARSET = Charset.forName("UTF-8");

   /**
    * In the particular case of Memcached, the end of operation/command
    * is signaled by "\r\n" characters. So, if end of operation is
    * found, this method would return true. On the contrary, if space was
    * found instead of end of operation character, then it'd return the element and false.
    */
   static boolean readElement(ByteBuf buffer, OutputStream byteBuffer) throws IOException {
      byte next = buffer.readByte();
      if (next == SP) { // Space
         return false;
      }
      else if (next == CR) { // CR
         next = buffer.readByte();
         if (next == LF) { // LF
            return true;
         } else {
            byteBuffer.write(next);
            return readElement(buffer, byteBuffer);
         }
      }
      else {
         byteBuffer.write(next);
         return readElement(buffer, byteBuffer);
      }
   }

   static String extractString(ByteArrayOutputStream byteBuffer) {
      String string = new String(byteBuffer.toByteArray(), CHARSET);
      byteBuffer.reset();
      return string;
   }

   static KeyValuePair<String, Boolean> readElement(ByteBuf buffer, StringBuilder sb) {
      byte next = buffer.readByte();
      if (next == SP) { // Space
         return new KeyValuePair<>(sb.toString().trim(), false);
      }
      else if (next == CR) { // CR
         next = buffer.readByte();
         if (next == LF) { // LF
            return new KeyValuePair<>(sb.toString().trim(), true);
         } else {
            sb.append(next);
            return readElement(buffer, sb);
         }
      }
      else {
         sb.append(next);
         return readElement(buffer, sb);
      }
   }

   static String readDiscardedLine(ByteBuf buffer) {
      if (readableBytes(buffer) > 0)
         return readDiscardedLine(buffer, new StringBuilder());
      else
         return "";
   }

   private static int readableBytes(ByteBuf buffer) {
      return buffer.writerIndex() - buffer.readerIndex();
   }

   private static String readDiscardedLine(ByteBuf buffer, StringBuilder sb) {
      byte next = buffer.readByte();
      if (next == CR) { // CR
         next = buffer.readByte();
         if (next == LF) { // LF
            return sb.toString().trim();
         } else {
            sb.append((char) next);
            return readDiscardedLine(buffer, sb);
         }
      } else if (next == LF) { //LF
         return sb.toString().trim();
      } else {
         sb.append((char) next);
         return readDiscardedLine(buffer, sb);
      }
   }

   static void skipLine(ByteBuf buffer) {
      if (readableBytes(buffer) > 0) {
         byte next = buffer.readByte();
         if (next == CR) { // CR
            next = buffer.readByte();
            if (next == LF) { // LF
               return;
            } else {
               skipLine(buffer);
            }
         } else if (next == LF) { //LF
            return;
         } else {
            skipLine(buffer);
         }
      }
   }

   static byte[] concat(byte[] a, byte[] b) {
       byte[] data = new byte[a.length + b.length];
       System.arraycopy(a, 0, data, 0, a.length);
       System.arraycopy(b, 0, data, a.length, b.length);
       return data;
   }

   static List<String> readSplitLine(ByteBuf buffer) {
      if (readableBytes(buffer) > 0)
         return readSplitLine(buffer, Stream.builder(), buffer.readerIndex(), 0).collect(Collectors.toList());
      else
         return Collections.emptyList();
   }

   private static Stream<String> readSplitLine(ByteBuf buffer, Stream.Builder<String> builder, int start, int length) {
      byte next = buffer.readByte();
      if (next == CR) { // CR
         next = buffer.readByte();
         if (next == LF) { // LF
            byte[] bytes = new byte[length];
            buffer.getBytes(start, bytes);
            builder.add(new String(bytes, CHARSET));
            return builder.build();
         } else {
            return readSplitLine(buffer, builder, start, length + 1);
         }
      } else if (next == LF) { // LF
         byte[] bytes = new byte[length];
         buffer.getBytes(start, bytes);
         builder.add(new String(bytes, CHARSET));
         return builder.build();
      } else if (next == SP) {
         byte[] bytes = new byte[length];
         buffer.getBytes(start, bytes);
         builder.add(new String(bytes, CHARSET));
         return readSplitLine(buffer, builder, start + length + 1, 0);
      } else {
         return readSplitLine(buffer, builder, start, length + 1);
      }
   }

}
