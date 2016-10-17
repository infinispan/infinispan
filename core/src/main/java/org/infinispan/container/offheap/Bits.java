package org.infinispan.container.offheap;

/**
 * Utility method for inserting and retrieving values from to/from a byte[]
 *
 * @author wburns
 * @since 9.0
 */
public class Bits {

   static int getInt(byte[] b, int off) {
      return ((b[off + 3] & 0xFF)) +
            ((b[off + 2] & 0xFF) << 8) +
            ((b[off + 1] & 0xFF) << 16) +
            ((b[off]) << 24);
   }

   static long getLong(byte[] b, int off) {
      return ((b[off + 7] & 0xFFL)) +
            ((b[off + 6] & 0xFFL) << 8) +
            ((b[off + 5] & 0xFFL) << 16) +
            ((b[off + 4] & 0xFFL) << 24) +
            ((b[off + 3] & 0xFFL) << 32) +
            ((b[off + 2] & 0xFFL) << 40) +
            ((b[off + 1] & 0xFFL) << 48) +
            (((long) b[off]) << 56);
   }

   static void putInt(byte[] b, int off, int val) {
      b[off + 3] = (byte) (val);
      b[off + 2] = (byte) (val >>> 8);
      b[off + 1] = (byte) (val >>> 16);
      b[off] = (byte) (val >>> 24);
   }

   static void putLong(byte[] b, int off, long val) {
      b[off + 7] = (byte) (val);
      b[off + 6] = (byte) (val >>> 8);
      b[off + 5] = (byte) (val >>> 16);
      b[off + 4] = (byte) (val >>> 24);
      b[off + 3] = (byte) (val >>> 32);
      b[off + 2] = (byte) (val >>> 40);
      b[off + 1] = (byte) (val >>> 48);
      b[off] = (byte) (val >>> 56);
   }
}
