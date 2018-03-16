package org.infinispan.server.hotrod;

public class BitShift {
   public static byte mask(byte value, int mask) {
      return (byte) (value & mask);
   }
   public static byte right(byte value, int num) {
      return (byte) ((0xFF & value) >> num);
   }
}
