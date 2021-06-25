package org.infinispan.commons.dataconversion;

public final class Base16Codec {

   private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

   private Base16Codec() {
   }

   public static String encode(byte[] content) {
      return bytesToHex(content);
   }

   public static byte[] decode(String content) {
      return hexToBytes(content);
   }

   private static String bytesToHex(byte[] bytes) {
      if (bytes == null) return null;
      if (bytes.length == 0) return "";
      StringBuilder r = new StringBuilder(bytes.length * 2);
      for (byte b : bytes) {
         r.append(HEX_DIGITS[b >> 4 & 0x0f]);
         r.append(HEX_DIGITS[b & 0x0f]);
      }
      return "0x" + r;
   }

   private static int forDigit(char digit) {
      if (digit >= '0' && digit <= '9') return digit - 48;
      if (digit == 'a') return 10;
      if (digit == 'b') return 11;
      if (digit == 'c') return 12;
      if (digit == 'd') return 13;
      if (digit == 'e') return 14;
      if (digit == 'f') return 15;
      throw new EncodingException("Invalid digit found in hex format!");
   }

   private static byte[] hexToBytes(String hex) {
      if (hex == null) return null;
      if (hex.isEmpty()) return new byte[]{};
      if (!hex.startsWith("0x") || hex.length() % 2 != 0) {
         throw new EncodingException("Illegal hex literal!");
      }
      byte[] result = new byte[(hex.length() - 2) / 2];
      for (int i = 2; i < hex.length(); i += 2) {
         int msb = forDigit(hex.charAt(i));
         int lsb = forDigit(hex.charAt(i + 1));
         byte b = (byte) (msb * 16 + lsb);
         result[(i - 2) / 2] = b;

      }
      return result;
   }
}
