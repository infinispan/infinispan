/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.util.hash;

import org.infinispan.marshall.Ids;
import org.infinispan.marshall.exts.NoStateExternalizer;
import org.infinispan.util.ByteArrayKey;
import org.infinispan.util.Util;

import java.io.ObjectInput;
import java.nio.charset.Charset;
import java.util.Set;

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
 * @since 5.0
 */

public class MurmurHash3 implements Hash {
   private static final Charset UTF8 = Charset.forName("UTF-8");

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
           (((long) key[i + 0] & 0x00000000000000FFL) << 0)
         | (((long) key[i + 1] & 0x00000000000000FFL) << 8)
         | (((long) key[i + 2] & 0x00000000000000FFL) << 16)
         | (((long) key[i + 3] & 0x00000000000000FFL) << 24)
         | (((long) key[i + 4] & 0x00000000000000FFL) << 32)
         | (((long) key[i + 5] & 0x00000000000000FFL) << 40)
         | (((long) key[i + 6] & 0x00000000000000FFL) << 48)
         | (((long) key[i + 7] & 0x00000000000000FFL) << 56);
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
         case 9:  state.k2 ^= (long) key[tail + 8] << 0;

         case 8:  state.k1 ^= (long) key[tail + 7] << 56;
         case 7:  state.k1 ^= (long) key[tail + 6] << 48;
         case 6:  state.k1 ^= (long) key[tail + 5] << 40;
         case 5:  state.k1 ^= (long) key[tail + 4] << 32;
         case 4:  state.k1 ^= (long) key[tail + 3] << 24;
         case 3:  state.k1 ^= (long) key[tail + 2] << 16;
         case 2:  state.k1 ^= (long) key[tail + 1] << 8;
         case 1:  state.k1 ^= (long) key[tail + 0] << 0;
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
      return MurmurHash3_x64_128(key, seed)[0];
   }

   /**
    * Hash a value using the x64 32 bit variant of MurmurHash3
    * 
    * @param key value to hash
    * @param seed random value
    * @return 32 bit hashed key
    */
   public static int MurmurHash3_x64_32(final byte[] key, final int seed) {
      return (int) (MurmurHash3_x64_128(key, seed)[0] >>> 32);
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
      return MurmurHash3_x64_128(key, seed)[0];
   }

   /**
    * Hash a value using the x64 32 bit variant of MurmurHash3
    * 
    * @param key value to hash
    * @param seed random value
    * @return 32 bit hashed key
    */
   public static int MurmurHash3_x64_32(final long[] key, final int seed) {
      return (int) (MurmurHash3_x64_128(key, seed)[0] >>> 32);
   }

   public int hash(byte[] payload) {
      return MurmurHash3_x64_32(payload, 9001);
   }

   /**
    * Hashes a byte array efficiently.
    * 
    * @param payload a byte array to hash
    * @return a hash code for the byte array
    */
   public int hash(long[] payload) {
      return MurmurHash3_x64_32(payload, 9001);
   }

   public int hash(int hashcode) {
      byte[] b = new byte[4];
      b[0] = (byte) hashcode;
      b[1] = (byte) (hashcode >>> 8);
      b[2] = (byte) (hashcode >>> 16);
      b[3] = (byte) (hashcode >>> 24);
      return hash(b);
   }

   public int hash(Object o) {
      if (o instanceof byte[])
         return hash((byte[]) o);
      else if (o instanceof long[])
         return hash((long[]) o);
      else if (o instanceof String)
         return hash(((String) o).getBytes(UTF8));
      else if (o instanceof ByteArrayKey)
         return hash(((ByteArrayKey) o).getData());
      else
         return hash(o.hashCode());
   }

   public static class Externalizer extends NoStateExternalizer<MurmurHash3> {
      @Override
      public Set<Class<? extends MurmurHash3>> getTypeClasses() {
         return Util.<Class<? extends MurmurHash3>>asSet(MurmurHash3.class);
      }

      @Override
      public MurmurHash3 readObject(ObjectInput input) {
         return new MurmurHash3();
      }

      @Override
      public Integer getId() {
         return Ids.MURMURHASH_3;
      }
   }


}
