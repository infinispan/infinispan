package org.infinispan.server.memcached;

public class ParseUtil {

   private ParseUtil() { }

   static final byte[] DigitTens = {
         '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
         '1', '1', '1', '1', '1', '1', '1', '1', '1', '1',
         '2', '2', '2', '2', '2', '2', '2', '2', '2', '2',
         '3', '3', '3', '3', '3', '3', '3', '3', '3', '3',
         '4', '4', '4', '4', '4', '4', '4', '4', '4', '4',
         '5', '5', '5', '5', '5', '5', '5', '5', '5', '5',
         '6', '6', '6', '6', '6', '6', '6', '6', '6', '6',
         '7', '7', '7', '7', '7', '7', '7', '7', '7', '7',
         '8', '8', '8', '8', '8', '8', '8', '8', '8', '8',
         '9', '9', '9', '9', '9', '9', '9', '9', '9', '9',
   };

   static final byte[] DigitOnes = {
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
   };

   public static byte[] ZERO = new byte[] { '0' };

   public static long readLong(byte[] in) {
      if (in == null || in.length == 0)
         throw new NumberFormatException("Empty argument");

      boolean negative = false;
      int i = 0;
      if (in[0] < '0') {
         if ((in[0] != '-' && in[0] != '+') || in.length == 1)
            throw new NumberFormatException("Invalid character: " + in[0]);

         negative = true;
         i = 1;
      }

      long result;

      byte b = in[i++];
      if (b < '0' || b > '9')
         throw new NumberFormatException("Invalid character: " + b);
      result = (b - 48);

      for (; i < in.length; i++) {
         b = in[i];
         if (b < '0' || b > '9')
            throw new NumberFormatException("Invalid character: " + b);

         result = (result << 3) + (result << 1) + (b - 48);
      }
      return negative ? -result : result;
   }

   public static int readInt(byte[] in) {
      long v = readLong(in);
      if (v > Integer.MAX_VALUE || v < Integer.MIN_VALUE) {
         throw new NumberFormatException("Invalid integer: " + v);
      }
      return (int)v;
   }


   public static byte[] writeAsciiLong(long in) {
      if (in == 0) return ZERO;

      int numberDigits = stringSize(in);
      int writerIndex = numberDigits;
      boolean negative = in < 0;
      byte[] out = new byte[numberDigits];

      if (!negative) {
         in = -in;
      }

      long q;
      int r;
      while (in <= -100) {
         q = in / 100;
         r = (int)((q * 100) - in);
         in = q;
         out[--writerIndex] = DigitOnes[r];
         out[--writerIndex] = DigitTens[r];
      }

      q = in / 10;
      r = (int)((q * 10) - in);
      out[--writerIndex] = (byte) ('0' + r);

      if (q < 0) {
         out[--writerIndex] = (byte) ('0' - q);
      }

      if (negative) {
         out[0] = '-';
      }

      return out;
   }

   private static int stringSize(long x) {
      int d = 1;
      if (x >= 0) {
         d = 0;
         x = -x;
      }
      int p = -10;
      for (int i = 1; i < 10; i++) {
         if (x > p)
            return i + d;
         p = 10 * p;
      }
      return 10 + d;
   }
}
