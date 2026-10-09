package org.infinispan.commons.hash;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import org.infinispan.commons.marshall.WrappedBytes;

import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.ThreadSafe;

/**
 * MurmurHash3 implementation in Java, based on Austin Appleby's <a href=
 * "https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp"
 * >original in C</a>
 * <p>
 * Only implementing x64 version, because this should always be faster on 64 bit
 * native processors, even 64 bit being ran with a 32 bit OS; this should also
 * be as fast or faster than the x86 version on some modern 32 bit processors.
 * <p>
 * <b>Note:</b> This implementation was ported from an early pre-release draft of
 * MurmurHash3 (circa late 2010) and differs from the finalized version in several ways:
 * <ul>
 *    <li>h1/h2 are initialized by XORing the seed with magic constants
 *        ({@code 0x9368e53c2f6af274} and {@code 0x586dcd208f7cd3fd}),
 *        whereas the final version initializes both to {@code seed}.</li>
 *    <li>c1/c2 are mutated each round ({@code c = c*5 + additive}),
 *        whereas the final version keeps them as fixed constants.</li>
 *    <li>Rotation amounts differ: this version uses 23/23/41,
 *        the final version uses 31/33/27/31.</li>
 *    <li>The h1/h2 update uses {@code h*3 + constant},
 *        the final version uses {@code h*5 + constant}.</li>
 * </ul>
 * These differences are permanent: the hash function is part of Infinispan's consistent
 * hashing protocol and wire format since 5.0. Changing it would break cluster compatibility
 * and data distribution. It is <b>not</b> bit-compatible with the canonical
 * {@code MurmurHash3_x64_128}.
 *
 * @author Patrick McFarland
 * @see <a href="http://sites.google.com/site/murmurhash/">MurmurHash website</a>
 * @see <a href="http://en.wikipedia.org/wiki/MurmurHash">MurmurHash entry on Wikipedia</a>
 * @since 5.0
 */
@ThreadSafe
@Immutable
public class MurmurHash3 implements Hash {
   private static final MurmurHash3 instance = new MurmurHash3();
   public static final byte INVALID_CHAR = (byte) '?';

   public static MurmurHash3 getInstance() {
      return instance;
   }

   private MurmurHash3() {
   }

   private static final VarHandle VH_LE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

   static long getblock(byte[] key, int i) {
      return (long) VH_LE_LONG.get(key, i);
   }

   static long fmix(long k) {
      k ^= k >>> 33;
      k *= 0xff51afd7ed558ccdL;
      k ^= k >>> 33;
      k *= 0xc4ceb9fe1a85ec53L;
      k ^= k >>> 33;

      return k;
   }

   /**
    * Hash a value using the x64 128 bit variant of MurmurHash3
    *
    * @param key  value to hash
    * @param seed random value
    * @return 128 bit hashed key, in an array containing two longs
    */
   public static long[] MurmurHash3_x64_128(final byte[] key, final int seed) {
      long h1 = 0x9368e53c2f6af274L ^ seed;
      long h2 = 0x586dcd208f7cd3fdL ^ seed;

      long c1 = 0x87c37b91114253d5L;
      long c2 = 0x4cf5ad432745937fL;

      int numBlocks = key.length / 16;
      for (int i = 0; i < numBlocks; i++) {
         long k1 = getblock(key, i * 2 * 8);
         long k2 = getblock(key, (i * 2 + 1) * 8);

         k1 *= c1;
         k1 = (k1 << 23) | (k1 >>> 41);
         k1 *= c2;
         h1 ^= k1;
         h1 += h2;

         h2 = (h2 << 41) | (h2 >>> 23);

         k2 *= c2;
         k2 = (k2 << 23) | (k2 >>> 41);
         k2 *= c1;
         h2 ^= k2;
         h2 += h1;

         h1 = h1 * 3 + 0x52dce729;
         h2 = h2 * 3 + 0x38495ab5;

         c1 = c1 * 5 + 0x7b7d159c;
         c2 = c2 * 5 + 0x6bce6396;
      }

      long k1 = 0;
      long k2 = 0;

      int tail = (key.length >>> 4) << 4;

      switch (key.length & 15) {
         case 15:
            k2 ^= (long) key[tail + 14] << 48;
         case 14:
            k2 ^= (long) key[tail + 13] << 40;
         case 13:
            k2 ^= (long) key[tail + 12] << 32;
         case 12:
            k2 ^= (long) key[tail + 11] << 24;
         case 11:
            k2 ^= (long) key[tail + 10] << 16;
         case 10:
            k2 ^= (long) key[tail + 9] << 8;
         case 9:
            k2 ^= key[tail + 8];

         case 8:
            k1 ^= (long) key[tail + 7] << 56;
         case 7:
            k1 ^= (long) key[tail + 6] << 48;
         case 6:
            k1 ^= (long) key[tail + 5] << 40;
         case 5:
            k1 ^= (long) key[tail + 4] << 32;
         case 4:
            k1 ^= (long) key[tail + 3] << 24;
         case 3:
            k1 ^= (long) key[tail + 2] << 16;
         case 2:
            k1 ^= (long) key[tail + 1] << 8;
         case 1:
            k1 ^= key[tail + 0];

            k1 *= c1;
            k1 = (k1 << 23) | (k1 >>> 41);
            k1 *= c2;
            h1 ^= k1;
            h1 += h2;

            h2 = (h2 << 41) | (h2 >>> 23);

            k2 *= c2;
            k2 = (k2 << 23) | (k2 >>> 41);
            k2 *= c1;
            h2 ^= k2;
            h2 += h1;

            h1 = h1 * 3 + 0x52dce729;
            h2 = h2 * 3 + 0x38495ab5;

            c1 = c1 * 5 + 0x7b7d159c;
            c2 = c2 * 5 + 0x6bce6396;
      }

      h2 ^= key.length;

      h1 += h2;
      h2 += h1;

      h1 = fmix(h1);
      h2 = fmix(h2);

      h1 += h2;
      h2 += h1;

      return new long[]{h1, h2};
   }

   /**
    * Hash a value using the x64 64 bit variant of MurmurHash3
    *
    * @param key  value to hash
    * @param seed random value
    * @return 64 bit hashed key
    */
   public static long MurmurHash3_x64_64(final byte[] key, final int seed) {
      long h1 = 0x9368e53c2f6af274L ^ seed;
      long h2 = 0x586dcd208f7cd3fdL ^ seed;

      long c1 = 0x87c37b91114253d5L;
      long c2 = 0x4cf5ad432745937fL;

      int numBlocks = key.length / 16;
      for (int i = 0; i < numBlocks; i++) {
         long k1 = getblock(key, i * 2 * 8);
         long k2 = getblock(key, (i * 2 + 1) * 8);

         k1 *= c1;
         k1 = (k1 << 23) | (k1 >>> 41);
         k1 *= c2;
         h1 ^= k1;
         h1 += h2;

         h2 = (h2 << 41) | (h2 >>> 23);

         k2 *= c2;
         k2 = (k2 << 23) | (k2 >>> 41);
         k2 *= c1;
         h2 ^= k2;
         h2 += h1;

         h1 = h1 * 3 + 0x52dce729;
         h2 = h2 * 3 + 0x38495ab5;

         c1 = c1 * 5 + 0x7b7d159c;
         c2 = c2 * 5 + 0x6bce6396;
      }

      long k1 = 0;
      long k2 = 0;

      int tail = (key.length >>> 4) << 4;

      switch (key.length & 15) {
         case 15:
            k2 ^= (long) key[tail + 14] << 48;
         case 14:
            k2 ^= (long) key[tail + 13] << 40;
         case 13:
            k2 ^= (long) key[tail + 12] << 32;
         case 12:
            k2 ^= (long) key[tail + 11] << 24;
         case 11:
            k2 ^= (long) key[tail + 10] << 16;
         case 10:
            k2 ^= (long) key[tail + 9] << 8;
         case 9:
            k2 ^= key[tail + 8];

         case 8:
            k1 ^= (long) key[tail + 7] << 56;
         case 7:
            k1 ^= (long) key[tail + 6] << 48;
         case 6:
            k1 ^= (long) key[tail + 5] << 40;
         case 5:
            k1 ^= (long) key[tail + 4] << 32;
         case 4:
            k1 ^= (long) key[tail + 3] << 24;
         case 3:
            k1 ^= (long) key[tail + 2] << 16;
         case 2:
            k1 ^= (long) key[tail + 1] << 8;
         case 1:
            k1 ^= key[tail + 0];

            k1 *= c1;
            k1 = (k1 << 23) | (k1 >>> 41);
            k1 *= c2;
            h1 ^= k1;
            h1 += h2;

            h2 = (h2 << 41) | (h2 >>> 23);

            k2 *= c2;
            k2 = (k2 << 23) | (k2 >>> 41);
            k2 *= c1;
            h2 ^= k2;
            h2 += h1;

            h1 = h1 * 3 + 0x52dce729;
            h2 = h2 * 3 + 0x38495ab5;

            c1 = c1 * 5 + 0x7b7d159c;
            c2 = c2 * 5 + 0x6bce6396;
      }

      h2 ^= key.length;

      h1 += h2;
      h2 += h1;

      h1 = fmix(h1);
      h2 = fmix(h2);

      h1 += h2;
      h2 += h1;

      return h1;
   }

   /**
    * Hash a value using the x64 32 bit variant of MurmurHash3
    *
    * @param key  value to hash
    * @param seed random value
    * @return 32 bit hashed key
    */
   public static int MurmurHash3_x64_32(final byte[] key, final int seed) {
      return (int) (MurmurHash3_x64_64(key, seed) >>> 32);
   }

   /**
    * Hash a value using the x64 128 bit variant of MurmurHash3
    *
    * @param key  value to hash
    * @param seed random value
    * @return 128 bit hashed key, in an array containing two longs
    */
   public static long[] MurmurHash3_x64_128(final long[] key, final int seed) {
      long h1 = 0x9368e53c2f6af274L ^ seed;
      long h2 = 0x586dcd208f7cd3fdL ^ seed;

      long c1 = 0x87c37b91114253d5L;
      long c2 = 0x4cf5ad432745937fL;

      long k1 = 0;
      long k2 = 0;

      for (int i = 0; i < key.length / 2; i++) {
         k1 = key[i * 2];
         k2 = key[i * 2 + 1];

         k1 *= c1;
         k1 = (k1 << 23) | (k1 >>> 41);
         k1 *= c2;
         h1 ^= k1;
         h1 += h2;

         h2 = (h2 << 41) | (h2 >>> 23);

         k2 *= c2;
         k2 = (k2 << 23) | (k2 >>> 41);
         k2 *= c1;
         h2 ^= k2;
         h2 += h1;

         h1 = h1 * 3 + 0x52dce729;
         h2 = h2 * 3 + 0x38495ab5;

         c1 = c1 * 5 + 0x7b7d159c;
         c2 = c2 * 5 + 0x6bce6396;
      }

      long tail = key[key.length - 1];

      // Key length is odd
      if ((key.length & 1) == 1) {
         k1 ^= tail;

         k1 *= c1;
         k1 = (k1 << 23) | (k1 >>> 41);
         k1 *= c2;
         h1 ^= k1;
         h1 += h2;

         h2 = (h2 << 41) | (h2 >>> 23);

         k2 *= c2;
         k2 = (k2 << 23) | (k2 >>> 41);
         k2 *= c1;
         h2 ^= k2;
         h2 += h1;

         h1 = h1 * 3 + 0x52dce729;
         h2 = h2 * 3 + 0x38495ab5;

         c1 = c1 * 5 + 0x7b7d159c;
         c2 = c2 * 5 + 0x6bce6396;
      }

      h2 ^= key.length * 8;

      h1 += h2;
      h2 += h1;

      h1 = fmix(h1);
      h2 = fmix(h2);

      h1 += h2;
      h2 += h1;

      return new long[]{h1, h2};
   }

   /**
    * Hash a value using the x64 64 bit variant of MurmurHash3
    *
    * @param key  value to hash
    * @param seed random value
    * @return 64 bit hashed key
    */
   public static long MurmurHash3_x64_64(final long[] key, final int seed) {
      long h1 = 0x9368e53c2f6af274L ^ seed;
      long h2 = 0x586dcd208f7cd3fdL ^ seed;

      long c1 = 0x87c37b91114253d5L;
      long c2 = 0x4cf5ad432745937fL;

      long k1 = 0;
      long k2 = 0;

      for (int i = 0; i < key.length / 2; i++) {
         k1 = key[i * 2];
         k2 = key[i * 2 + 1];

         k1 *= c1;
         k1 = (k1 << 23) | (k1 >>> 41);
         k1 *= c2;
         h1 ^= k1;
         h1 += h2;

         h2 = (h2 << 41) | (h2 >>> 23);

         k2 *= c2;
         k2 = (k2 << 23) | (k2 >>> 41);
         k2 *= c1;
         h2 ^= k2;
         h2 += h1;

         h1 = h1 * 3 + 0x52dce729;
         h2 = h2 * 3 + 0x38495ab5;

         c1 = c1 * 5 + 0x7b7d159c;
         c2 = c2 * 5 + 0x6bce6396;
      }

      long tail = key[key.length - 1];

      if (key.length % 2 != 0) {
         k1 ^= tail;

         k1 *= c1;
         k1 = (k1 << 23) | (k1 >>> 41);
         k1 *= c2;
         h1 ^= k1;
         h1 += h2;

         h2 = (h2 << 41) | (h2 >>> 23);

         k2 *= c2;
         k2 = (k2 << 23) | (k2 >>> 41);
         k2 *= c1;
         h2 ^= k2;
         h2 += h1;

         h1 = h1 * 3 + 0x52dce729;
         h2 = h2 * 3 + 0x38495ab5;

         c1 = c1 * 5 + 0x7b7d159c;
         c2 = c2 * 5 + 0x6bce6396;
      }

      h2 ^= key.length * 8;

      h1 += h2;
      h2 += h1;

      h1 = fmix(h1);
      h2 = fmix(h2);

      h1 += h2;
      h2 += h1;

      return h1;
   }

   /**
    * Hash a value using the x64 32 bit variant of MurmurHash3
    *
    * @param key  value to hash
    * @param seed random value
    * @return 32 bit hashed key
    */
   public static int MurmurHash3_x64_32(final long[] key, final int seed) {
      return (int) (MurmurHash3_x64_64(key, seed) >>> 32);
   }

   @Override
   public int hash(byte[] payload) {
      return MurmurHash3_x64_32(payload, 9001);
   }

   /**
    * Hashes a byte array efficiently.
    *
    * @param payload a byte array to hash
    * @return a hash code for the byte array
    */
   public static int hash(long[] payload) {
      return MurmurHash3_x64_32(payload, 9001);
   }

   @Override
   public int hash(int hashcode) {
      // Obtained by inlining MurmurHash3_x64_32(byte[], 9001) and removing all the unused code
      // (since we know the input is always 4 bytes and we only need 4 bytes of output)
      byte b0 = (byte) hashcode;
      byte b1 = (byte) (hashcode >>> 8);
      byte b2 = (byte) (hashcode >>> 16);
      byte b3 = (byte) (hashcode >>> 24);

      long h1 = 0x9368e53c2f6af274L ^ 9001;
      long h2 = 0x586dcd208f7cd3fdL ^ 9001;

      long c1 = 0x87c37b91114253d5L;
      long c2 = 0x4cf5ad432745937fL;

      long k1 = 0;
      long k2 = 0;

      k1 ^= (long) b3 << 24;
      k1 ^= (long) b2 << 16;
      k1 ^= (long) b1 << 8;
      k1 ^= b0;

      k1 *= c1;
      k1 = (k1 << 23) | (k1 >>> 41);
      k1 *= c2;
      h1 ^= k1;
      h1 += h2;

      h2 = (h2 << 41) | (h2 >>> 23);

      k2 *= c2;
      k2 = (k2 << 23) | (k2 >>> 41);
      k2 *= c1;
      h2 ^= k2;
      h2 += h1;

      h1 = h1 * 3 + 0x52dce729;
      h2 = h2 * 3 + 0x38495ab5;

      c1 = c1 * 5 + 0x7b7d159c;
      c2 = c2 * 5 + 0x6bce6396;

      h2 ^= 4;

      h1 += h2;
      h2 += h1;

      h1 = fmix(h1);
      h2 = fmix(h2);

      h1 += h2;
      h2 += h1;

      return (int) (h1 >>> 32);
   }

   @Override
   public int hash(Object o) {
      if (o instanceof byte[])
         return hash((byte[]) o);
      else if (o instanceof WrappedBytes) {
         return hash(((WrappedBytes) o).getBytes());
      } else if (o instanceof long[])
         return hash((long[]) o);
      else if (o instanceof String)
         return hashString((String) o);
      else
         return hash(o.hashCode());
   }

   private int hashString(String s) {
      return (int) (MurmurHash3_x64_64_String(s, 9001) >> 32);
   }

   private long MurmurHash3_x64_64_String(String s, long seed) {
      // Exactly the same as MurmurHash3_x64_64, except it works directly on a String's chars
      long h1 = 0x9368e53c2f6af274L ^ seed;
      long h2 = 0x586dcd208f7cd3fdL ^ seed;

      long c1 = 0x87c37b91114253d5L;
      long c2 = 0x4cf5ad432745937fL;

      long k1 = 0;
      long k2 = 0;
      int byteLen = 0;
      int stringLen = s.length();
      for (int i = 0; i < stringLen; i++) {
         char c1Char = s.charAt(i);
         if (c1Char <= 0x7f) {
            int shift = (byteLen & 0x7) * 8;
            long bb = ((long) c1Char) << shift;
            if ((byteLen & 0x8) == 0) {
               k1 |= bb;
            } else {
               k2 |= bb;
               if ((byteLen & 0xf) == 0xf) {
                  k1 *= c1;
                  k1 = (k1 << 23) | (k1 >>> 41);
                  k1 *= c2;
                  h1 ^= k1;
                  h1 += h2;

                  h2 = (h2 << 41) | (h2 >>> 23);

                  k2 *= c2;
                  k2 = (k2 << 23) | (k2 >>> 41);
                  k2 *= c1;
                  h2 ^= k2;
                  h2 += h1;

                  h1 = h1 * 3 + 0x52dce729;
                  h2 = h2 * 3 + 0x38495ab5;

                  c1 = c1 * 5 + 0x7b7d159c;
                  c2 = c2 * 5 + 0x6bce6396;

                  k1 = 0;
                  k2 = 0;
               }
            }
            byteLen++;
         } else {
            int cp;
            if (!Character.isSurrogate(c1Char)) {
               cp = c1Char;
            } else if (Character.isHighSurrogate(c1Char)) {
               if (i + 1 < stringLen) {
                  char c2Char = s.charAt(i + 1);
                  if (Character.isLowSurrogate(c2Char)) {
                     i++;
                     cp = Character.toCodePoint(c1Char, c2Char);
                  } else {
                     cp = INVALID_CHAR;
                  }
               } else {
                  cp = INVALID_CHAR;
               }
            } else {
               cp = INVALID_CHAR;
            }

            int bCount;
            int b0 = 0;
            int b1 = 0;
            int b2 = 0;
            int b3 = 0;
            if (cp <= 0x7f) {
               b0 = cp;
               bCount = 1;
            } else if (cp <= 0x07ff) {
               b0 = 0xc0 | (0x1f & (cp >> 6));
               b1 = 0x80 | (0x3f & cp);
               bCount = 2;
            } else if (cp <= 0xffff) {
               b0 = 0xe0 | (0x0f & (cp >> 12));
               b1 = 0x80 | (0x3f & (cp >> 6));
               b2 = 0x80 | (0x3f & cp);
               bCount = 3;
            } else {
               b0 = 0xf0 | (0x07 & (cp >> 18));
               b1 = 0x80 | (0x3f & (cp >> 12));
               b2 = 0x80 | (0x3f & (cp >> 6));
               b3 = 0x80 | (0x3f & cp);
               bCount = 4;
            }

            for (int bIdx = 0; bIdx < bCount; bIdx++) {
               int b = (bIdx == 0) ? b0 : (bIdx == 1) ? b1 : (bIdx == 2) ? b2 : b3;
               int shift = (byteLen & 0x7) * 8;
               long bb = (b & 0xffL) << shift;
               if ((byteLen & 0x8) == 0) {
                  k1 |= bb;
               } else {
                  k2 |= bb;
                  if ((byteLen & 0xf) == 0xf) {
                     k1 *= c1;
                     k1 = (k1 << 23) | (k1 >>> 41);
                     k1 *= c2;
                     h1 ^= k1;
                     h1 += h2;

                     h2 = (h2 << 41) | (h2 >>> 23);

                     k2 *= c2;
                     k2 = (k2 << 23) | (k2 >>> 41);
                     k2 *= c1;
                     h2 ^= k2;
                     h2 += h1;

                     h1 = h1 * 3 + 0x52dce729;
                     h2 = h2 * 3 + 0x38495ab5;

                     c1 = c1 * 5 + 0x7b7d159c;
                     c2 = c2 * 5 + 0x6bce6396;

                     k1 = 0;
                     k2 = 0;
                  }
               }
               byteLen++;
            }
         }
      }

      long savedK1 = k1;
      long savedK2 = k2;
      k1 = 0;
      k2 = 0;
      switch (byteLen & 15) {
         case 15:
            k2 ^= (long) ((byte) (savedK2 >> 48)) << 48;
         case 14:
            k2 ^= (long) ((byte) (savedK2 >> 40)) << 40;
         case 13:
            k2 ^= (long) ((byte) (savedK2 >> 32)) << 32;
         case 12:
            k2 ^= (long) ((byte) (savedK2 >> 24)) << 24;
         case 11:
            k2 ^= (long) ((byte) (savedK2 >> 16)) << 16;
         case 10:
            k2 ^= (long) ((byte) (savedK2 >> 8)) << 8;
         case 9:
            k2 ^= ((byte) savedK2);

         case 8:
            k1 ^= (long) ((byte) (savedK1 >> 56)) << 56;
         case 7:
            k1 ^= (long) ((byte) (savedK1 >> 48)) << 48;
         case 6:
            k1 ^= (long) ((byte) (savedK1 >> 40)) << 40;
         case 5:
            k1 ^= (long) ((byte) (savedK1 >> 32)) << 32;
         case 4:
            k1 ^= (long) ((byte) (savedK1 >> 24)) << 24;
         case 3:
            k1 ^= (long) ((byte) (savedK1 >> 16)) << 16;
         case 2:
            k1 ^= (long) ((byte) (savedK1 >> 8)) << 8;
         case 1:
            k1 ^= ((byte) savedK1);

            k1 *= c1;
            k1 = (k1 << 23) | (k1 >>> 41);
            k1 *= c2;
            h1 ^= k1;
            h1 += h2;

            h2 = (h2 << 41) | (h2 >>> 23);

            k2 *= c2;
            k2 = (k2 << 23) | (k2 >>> 41);
            k2 *= c1;
            h2 ^= k2;
            h2 += h1;

            h1 = h1 * 3 + 0x52dce729;
            h2 = h2 * 3 + 0x38495ab5;

            c1 = c1 * 5 + 0x7b7d159c;
            c2 = c2 * 5 + 0x6bce6396;
      }

      h2 ^= byteLen;

      h1 += h2;
      h2 += h1;

      h1 = fmix(h1);
      h2 = fmix(h2);

      h1 += h2;
      h2 += h1;

      return h1;
   }

   @Override
   public boolean equals(Object other) {
      return other != null && other.getClass() == getClass();
   }

   @Override
   public int hashCode() {
      return 0;
   }

   @Override
   public String toString() {
      return "MurmurHash3";
   }
}
