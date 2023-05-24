package org.infinispan.server.resp.commands;

/**
 * Utility class to transform byte[] arguments.
 *
 * @since 15.0
 */
public final class ArgumentUtils {

   private ArgumentUtils() {

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
}
