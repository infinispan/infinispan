package org.infinispan.server.resp.commands;

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

      return Double.parseDouble(new String(argument, offset, argument.length - offset, CharsetUtil.US_ASCII));
   }

   public static double toDouble(byte[] argument) {
      if (argument == null || argument.length == 0)
         throw new NumberFormatException("Empty argument");

      return Double.parseDouble(toNumberString(argument));
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

   /**
    * Checks if a possible numeric argument is "-inf".
    * @param arg
    * @return true if byte[] is -inf
    */
   public static boolean isNegativeInf(byte[] arg) {
      if (arg.length != 4)
         return false;

      return arg[0] == (byte) '-' && arg[1] == (byte) 'i' && arg[2] == (byte) 'n' && arg[3] == (byte) 'f';
   }

   /**
    * Checks if a possible numeric argument is "+inf".
    * @param arg
    * @return true if byte[] is +inf
    */
   public static boolean isPositiveInf(byte[] arg) {
      if (arg.length != 4)
         return false;

      return arg[0] == (byte) '+' && arg[1] == (byte) 'i' && arg[2] == (byte) 'n' && arg[3] == (byte) 'f';
   }
}
