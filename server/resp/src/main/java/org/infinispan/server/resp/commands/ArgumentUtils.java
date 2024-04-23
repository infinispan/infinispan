package org.infinispan.server.resp.commands;

import java.nio.charset.StandardCharsets;

import org.infinispan.server.resp.ByteBufferUtils;

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

      return toDoubleWithInfinity(new String(argument, offset, argument.length - offset, CharsetUtil.US_ASCII));
   }

   public static double toDouble(byte[] argument) {
      if (argument == null || argument.length == 0)
         throw new NumberFormatException("Empty argument");
      String sArg = toNumberString(argument);
      return toDoubleWithInfinity(sArg);
   }

   private static double toDoubleWithInfinity(String sArg) {
      if (sArg.equalsIgnoreCase("Inf") || sArg.equalsIgnoreCase("+Inf")) {
      sArg="Infinity";
      }
      if (sArg.equalsIgnoreCase("-Inf")) {
      sArg="-Infinity";
      }
      return Double.parseDouble(sArg);
   }

   public static long toLong(byte[] argument) {
      if (argument == null || argument.length == 0)
         throw new NumberFormatException("Empty argument");

      boolean negative = false;
      int i = 0;
      if (argument[0] < '0') {
         if ((argument[0] != '-' && argument[0] != '+') || argument.length == 1)
            throw new NumberFormatException("Invalid character: " + argument[0]);

         negative = true;
         i = 1;
      }

      long result;
      byte b = argument[i++];
      if (b < '0' || b > '9')
         throw new NumberFormatException("Invalid character: " + b);

      result = (b - 48);
      for (; i < argument.length; i++) {
         b = argument[i];
         if (b < '0' || b > '9')
            throw new NumberFormatException("Invalid character: " + b);

         result = (result << 3) + (result << 1) + (b - 48);
      }
      return negative ? -result : result;
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
}
