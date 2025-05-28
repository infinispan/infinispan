package org.infinispan.server.resp.commands;

import java.nio.charset.StandardCharsets;

import org.infinispan.server.resp.serialization.bytebuf.ByteBufferUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

/**
 * Utility class to transform byte[] arguments.
 *
 * @since 15.0
 */
public final class ArgumentUtils {

   private ArgumentUtils() {

   }

   /**
    * Numbers are always ASCII.
    *
    * @param argument, byte[]
    * @return String
    */
   public static String toNumberString(byte[] argument) {
      return new String(argument, CharsetUtil.US_ASCII);
   }

   /**
    * Parse Double, removing an offset from the argument
    * @param argument
    * @param offset, starting from
    * @return double value
    */
   public static double toDouble(byte[] argument, int offset) {
      if (argument == null || argument.length == 0)
         throw new NumberFormatException("Empty argument");

      String s = new String(argument, offset, argument.length - offset, CharsetUtil.US_ASCII);
      return toDoubleWithInfinity(s, argument, offset);
   }

   public static double toDouble(byte[] argument) {
      if (argument == null || argument.length == 0)
         throw new NumberFormatException("Empty argument");
      String sArg = toNumberString(argument);
      return toDoubleWithInfinity(sArg, argument, 0);
   }

   private static double toDoubleWithInfinity(String sArg, byte[] number, int offset) {
      if ("Inf".equalsIgnoreCase(sArg)
            || "+Inf".equalsIgnoreCase(sArg)
            || "Infinity".equals(sArg)) {
         return Double.POSITIVE_INFINITY;
      }

      if ("-Inf".equalsIgnoreCase(sArg) || "-Infinity".equals(sArg)) {
         return Double.NEGATIVE_INFINITY;
      }

      assertNumericString(number, offset);
      return Double.parseDouble(sArg);
   }

   private static void assertNumericString(byte[] number, int offset) {
      for (int i = offset; i < number.length; i++) {
         byte b = number[i];
         if (!isNumber(b))
            throw new NumberFormatException("Value is not a number");
      }
   }

   private static boolean isNumber(byte b) {
      return (b >= '0' && b <= '9') || isNumberSymbol(b);
   }

   private static boolean isNumberSymbol(byte b) {
      return b == '-' || b == '+' || b == '.' || b == 'e' || b == 'E';
   }

   public static long toLong(byte[] argument) {
      if (argument == null || argument.length == 0)
         throw new NumberFormatException("Empty argument");

      boolean negative = false;
      int i = 0;
      if (argument[0] < '0') {
         if ((argument[0] != '-' && argument[0] != '+') || argument.length == 1)
            throw new NumberFormatException("Invalid character: " + argument[0]);
         if (argument[0] == '-') {
            negative = true;
         }
         i = 1;
      }

      long result = 0;
      byte b = argument[i++];
      if (b < '0' || b > '9')
         throw new NumberFormatException("Invalid character: " + b);
      // Summing negative number (48-b) so we can handle -9223372036854775808
      result = (result << 3) + (result << 1) + (48 - b);

      for (; i < argument.length; i++) {
         b = argument[i];
         if (b < '0' || b > '9')
            throw new NumberFormatException("Invalid character: " + b);
         result = (result << 3) + (result << 1) + (48 - b);
         // Check if overflow happened, result*10 turns > 0
         if (result >= -9) {
            throw new NumberFormatException("Value out of range: " + ArgumentUtils.toNumberString(argument));
         }
      }

      if (negative) {
         return result;
      }
      // Check if can't change sign
      if (result == Long.MIN_VALUE) {
         throw new NumberFormatException("Value out of range: " + ArgumentUtils.toNumberString(argument));
      }
      return -result;
   }

   public static int toInt(byte[] argument) {
      long v = toLong(argument);
      if (v > Integer.MAX_VALUE || v < Integer.MIN_VALUE)
         throw new NumberFormatException("Value out of range: " + v);
      return (int) v;
   }

   public static byte[] toByteArray(long value) {
      int size = ByteBufferUtils.stringSize(value);
      ByteBuf buf = Unpooled.wrappedBuffer(new byte[size]);
      buf.resetWriterIndex();
      ByteBufferUtils.setIntChars(value, size, buf);
      return buf.array();
   }

   public static byte[] toByteArray(Number value) {
      double d = value.doubleValue();
      if (value instanceof Double && Math.rint(d) != d) {
         return Double.toString(d).getBytes(StandardCharsets.US_ASCII);
      }
      return toByteArray(value.longValue());
   }

   public static boolean isFloatValid(double value) {
      return !Double.isNaN(value) && !Double.isInfinite(value);
   }
}
