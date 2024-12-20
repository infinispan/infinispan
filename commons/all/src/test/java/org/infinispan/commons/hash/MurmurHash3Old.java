package org.infinispan.commons.hash;

import java.nio.charset.StandardCharsets;

import net.jcip.annotations.Immutable;
import net.jcip.annotations.ThreadSafe;

/**
 * MurmurHash3 implementation in Java, based on Austin Appleby's <a href=
 * "https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp"
 * >original in C</a>
 *
 * Only implementing x64 version, because this should always be faster on 64 bit
 * native processors, even 64 bit being ran with a 32 bit OS; this should also
 * be as fast or faster than the x86 version on some modern 32 bit processors.
 *
 * @author Patrick McFarland
 * @see <a href="http://sites.google.com/site/murmurhash/">MurmurHash website</a>
 * @see <a href="http://en.wikipedia.org/wiki/MurmurHash">MurmurHash entry on Wikipedia</a>
 */
@ThreadSafe
@Immutable
public class MurmurHash3Old implements Hash {
   private final static MurmurHash3Old instance = new MurmurHash3Old();

   public static MurmurHash3Old getInstance() {
      return instance;
   }

   private MurmurHash3Old() {
   }

   static class State {
      long h1;
      long h2;

      long k1;
      long k2;

      long c1;
      long c2;
   }

   static long getblock(byte[] key, int i) {
      return
           ((key[i + 0] & 0x00000000000000FFL))
         | ((key[i + 1] & 0x00000000000000FFL) << 8)
         | ((key[i + 2] & 0x00000000000000FFL) << 16)
         | ((key[i + 3] & 0x00000000000000FFL) << 24)
         | ((key[i + 4] & 0x00000000000000FFL) << 32)
         | ((key[i + 5] & 0x00000000000000FFL) << 40)
         | ((key[i + 6] & 0x00000000000000FFL) << 48)
         | ((key[i + 7] & 0x00000000000000FFL) << 56);
   }

   static void bmix(State state) {
      state.k1 *= state.c1;
      state.k1 = (state.k1 << 23) | (state.k1 >>> 64 - 23);
      state.k1 *= state.c2;
      state.h1 ^= state.k1;
      state.h1 += state.h2;

      state.h2 = (state.h2 << 41) | (state.h2 >>> 64 - 41);

      state.k2 *= state.c2;
      state.k2 = (state.k2 << 23) | (state.k2 >>> 64 - 23);
      state.k2 *= state.c1;
      state.h2 ^= state.k2;
      state.h2 += state.h1;

      state.h1 = state.h1 * 3 + 0x52dce729;
      state.h2 = state.h2 * 3 + 0x38495ab5;

      state.c1 = state.c1 * 5 + 0x7b7d159c;
      state.c2 = state.c2 * 5 + 0x6bce6396;
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
    * @param key value to hash
    * @param seed random value
    * @return 128 bit hashed key, in an array containing two longs
    */
   public static long[] MurmurHash3_x64_128(final byte[] key, final int seed) {
      State state = new State();

      state.h1 = 0x9368e53c2f6af274L ^ seed;
      state.h2 = 0x586dcd208f7cd3fdL ^ seed;

      state.c1 = 0x87c37b91114253d5L;
      state.c2 = 0x4cf5ad432745937fL;

      for (int i = 0; i < key.length / 16; i++) {
         state.k1 = getblock(key, i * 2 * 8);
         state.k2 = getblock(key, (i * 2 + 1) * 8);

         bmix(state);
      }

      state.k1 = 0;
      state.k2 = 0;

      int tail = (key.length >>> 4) << 4;

      switch (key.length & 15) {
         case 15: state.k2 ^= (long) key[tail + 14] << 48;
         case 14: state.k2 ^= (long) key[tail + 13] << 40;
         case 13: state.k2 ^= (long) key[tail + 12] << 32;
         case 12: state.k2 ^= (long) key[tail + 11] << 24;
         case 11: state.k2 ^= (long) key[tail + 10] << 16;
         case 10: state.k2 ^= (long) key[tail + 9] << 8;
         case 9:  state.k2 ^= key[tail + 8];

         case 8:  state.k1 ^= (long) key[tail + 7] << 56;
         case 7:  state.k1 ^= (long) key[tail + 6] << 48;
         case 6:  state.k1 ^= (long) key[tail + 5] << 40;
         case 5:  state.k1 ^= (long) key[tail + 4] << 32;
         case 4:  state.k1 ^= (long) key[tail + 3] << 24;
         case 3:  state.k1 ^= (long) key[tail + 2] << 16;
         case 2:  state.k1 ^= (long) key[tail + 1] << 8;
         case 1:  state.k1 ^= key[tail + 0];
            bmix(state);
      }

      state.h2 ^= key.length;

      state.h1 += state.h2;
      state.h2 += state.h1;

      state.h1 = fmix(state.h1);
      state.h2 = fmix(state.h2);

      state.h1 += state.h2;
      state.h2 += state.h1;

      return new long[] { state.h1, state.h2 };
   }

   /**
    * Hash a value using the x64 64 bit variant of MurmurHash3
    *
    * @param key value to hash
    * @param seed random value
    * @return 64 bit hashed key
    */
   public static long MurmurHash3_x64_64(final byte[] key, final int seed) {
      // Exactly the same as MurmurHash3_x64_128, except it only returns state.h1
      State state = new State();

      state.h1 = 0x9368e53c2f6af274L ^ seed;
      state.h2 = 0x586dcd208f7cd3fdL ^ seed;

      state.c1 = 0x87c37b91114253d5L;
      state.c2 = 0x4cf5ad432745937fL;

      for (int i = 0; i < key.length / 16; i++) {
         state.k1 = getblock(key, i * 2 * 8);
         state.k2 = getblock(key, (i * 2 + 1) * 8);

         bmix(state);
      }

      state.k1 = 0;
      state.k2 = 0;

      int tail = (key.length >>> 4) << 4;

      switch (key.length & 15) {
         case 15: state.k2 ^= (long) key[tail + 14] << 48;
         case 14: state.k2 ^= (long) key[tail + 13] << 40;
         case 13: state.k2 ^= (long) key[tail + 12] << 32;
         case 12: state.k2 ^= (long) key[tail + 11] << 24;
         case 11: state.k2 ^= (long) key[tail + 10] << 16;
         case 10: state.k2 ^= (long) key[tail + 9] << 8;
         case 9:  state.k2 ^= key[tail + 8];

         case 8:  state.k1 ^= (long) key[tail + 7] << 56;
         case 7:  state.k1 ^= (long) key[tail + 6] << 48;
         case 6:  state.k1 ^= (long) key[tail + 5] << 40;
         case 5:  state.k1 ^= (long) key[tail + 4] << 32;
         case 4:  state.k1 ^= (long) key[tail + 3] << 24;
         case 3:  state.k1 ^= (long) key[tail + 2] << 16;
         case 2:  state.k1 ^= (long) key[tail + 1] << 8;
         case 1:  state.k1 ^= key[tail + 0];
            bmix(state);
      }

      state.h2 ^= key.length;

      state.h1 += state.h2;
      state.h2 += state.h1;

      state.h1 = fmix(state.h1);
      state.h2 = fmix(state.h2);

      state.h1 += state.h2;
      state.h2 += state.h1;

      return state.h1;
   }

   /**
    * Hash a value using the x64 32 bit variant of MurmurHash3
    *
    * @param key value to hash
    * @param seed random value
    * @return 32 bit hashed key
    */
   public static int MurmurHash3_x64_32(final byte[] key, final int seed) {
      return (int) (MurmurHash3_x64_64(key, seed) >>> 32);
   }

   /**
    * Hash a value using the x64 128 bit variant of MurmurHash3
    *
    * @param key value to hash
    * @param seed random value
    * @return 128 bit hashed key, in an array containing two longs
    */
   public static long[] MurmurHash3_x64_128(final long[] key, final int seed) {
      State state = new State();

      state.h1 = 0x9368e53c2f6af274L ^ seed;
      state.h2 = 0x586dcd208f7cd3fdL ^ seed;

      state.c1 = 0x87c37b91114253d5L;
      state.c2 = 0x4cf5ad432745937fL;

      for (int i = 0; i < key.length / 2; i++) {
         state.k1 = key[i * 2];
         state.k2 = key[i * 2 + 1];

         bmix(state);
      }

      long tail = key[key.length - 1];

      // Key length is odd
      if ((key.length & 1) == 1) {
         state.k1 ^= tail;
         bmix(state);
      }

      state.h2 ^= key.length * 8;

      state.h1 += state.h2;
      state.h2 += state.h1;

      state.h1 = fmix(state.h1);
      state.h2 = fmix(state.h2);

      state.h1 += state.h2;
      state.h2 += state.h1;

      return new long[] { state.h1, state.h2 };
   }

   /**
    * Hash a value using the x64 64 bit variant of MurmurHash3
    *
    * @param key value to hash
    * @param seed random value
    * @return 64 bit hashed key
    */
   public static long MurmurHash3_x64_64(final long[] key, final int seed) {
      // Exactly the same as MurmurHash3_x64_128, except it only returns state.h1
      State state = new State();

      state.h1 = 0x9368e53c2f6af274L ^ seed;
      state.h2 = 0x586dcd208f7cd3fdL ^ seed;

      state.c1 = 0x87c37b91114253d5L;
      state.c2 = 0x4cf5ad432745937fL;

      for (int i = 0; i < key.length / 2; i++) {
         state.k1 = key[i * 2];
         state.k2 = key[i * 2 + 1];

         bmix(state);
      }

      long tail = key[key.length - 1];

      if (key.length % 2 != 0) {
         state.k1 ^= tail;
         bmix(state);
      }

      state.h2 ^= key.length * 8;

      state.h1 += state.h2;
      state.h2 += state.h1;

      state.h1 = fmix(state.h1);
      state.h2 = fmix(state.h2);

      state.h1 += state.h2;
      state.h2 += state.h1;

      return state.h1;
   }

   /**
    * Hash a value using the x64 32 bit variant of MurmurHash3
    *
    * @param key value to hash
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
      State state = new State();

      state.h1 = 0x9368e53c2f6af274L ^ 9001;
      state.h2 = 0x586dcd208f7cd3fdL ^ 9001;

      state.c1 = 0x87c37b91114253d5L;
      state.c2 = 0x4cf5ad432745937fL;

      state.k1 = 0;
      state.k2 = 0;

      state.k1 ^= (long) b3 << 24;
      state.k1 ^= (long) b2 << 16;
      state.k1 ^= (long) b1 << 8;
      state.k1 ^= b0;
      bmix(state);

      state.h2 ^= 4;

      state.h1 += state.h2;
      state.h2 += state.h1;

      state.h1 = fmix(state.h1);
      state.h2 = fmix(state.h2);

      state.h1 += state.h2;
      state.h2 += state.h1;

      return (int) (state.h1 >>> 32);
   }

   @Override
   public int hash(Object o) {
      if (o instanceof byte[])
         return hash((byte[]) o);
      else if (o instanceof long[])
         return hash((long[]) o);
      else if (o instanceof String)
         return hash(((String) o).getBytes(StandardCharsets.UTF_8));
      else
         return hash(o.hashCode());
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
