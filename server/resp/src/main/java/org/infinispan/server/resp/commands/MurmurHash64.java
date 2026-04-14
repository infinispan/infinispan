package org.infinispan.server.resp.commands;

/**
 * MurmurHash3-based 64-bit hash used by probabilistic data structures (CuckooFilter, CountMinSketch).
 * This implementation differs from {@link org.infinispan.commons.hash.MurmurHash3}.
 */
public final class MurmurHash64 {

   private MurmurHash64() {
   }

   public static long hash(byte[] data, int seed) {
      final long c1 = 0x87c37b91114253d5L;
      final long c2 = 0x4cf5ad432745937fL;

      long h = seed;
      int len = data.length;
      int i = 0;

      while (i + 8 <= len) {
         long k = getLongLE(data, i);
         k *= c1;
         k = Long.rotateLeft(k, 31);
         k *= c2;
         h ^= k;
         h = Long.rotateLeft(h, 27);
         h = h * 5 + 0x52dce729;
         i += 8;
      }

      long k = 0;
      int remaining = len - i;
      if (remaining > 0) {
         for (int j = remaining - 1; j >= 0; j--) {
            k <<= 8;
            k |= (data[i + j] & 0xFFL);
         }
         k *= c1;
         k = Long.rotateLeft(k, 31);
         k *= c2;
         h ^= k;
      }

      h ^= len;
      h = fmix64(h);
      return h;
   }

   private static long getLongLE(byte[] data, int offset) {
      return (data[offset] & 0xFFL)
            | ((data[offset + 1] & 0xFFL) << 8)
            | ((data[offset + 2] & 0xFFL) << 16)
            | ((data[offset + 3] & 0xFFL) << 24)
            | ((data[offset + 4] & 0xFFL) << 32)
            | ((data[offset + 5] & 0xFFL) << 40)
            | ((data[offset + 6] & 0xFFL) << 48)
            | ((data[offset + 7] & 0xFFL) << 56);
   }

   private static long fmix64(long h) {
      h ^= h >>> 33;
      h *= 0xff51afd7ed558ccdL;
      h ^= h >>> 33;
      h *= 0xc4ceb9fe1a85ec53L;
      h ^= h >>> 33;
      return h;
   }
}
