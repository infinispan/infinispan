package org.infinispan.util.hash;

import org.infinispan.util.ByteArrayKey;

/**
 * An implementation of Austin Appleby's MurmurHash2.0 algorithm, as documented on <a href="http://sites.google.com/site/murmurhash/">his website</a>.
 * <p />
 * This implementation is based on the slower, endian-neutral version of the algorithm as documented on the site,
 * ported from Austin Appleby's original C++ version <a href="http://sites.google.com/site/murmurhash/MurmurHashNeutral2.cpp">MurmurHashNeutral2.cpp</a>.
 * <p />
 * Other implementations are documented on Wikipedia's <a href="http://en.wikipedia.org/wiki/MurmurHash">MurmurHash</a> page.
 * <p />
 * @see <a href="http://sites.google.com/site/murmurhash/">MurmurHash website</a>
 * @see <a href="http://en.wikipedia.org/wiki/MurmurHash/">MurmurHash entry on Wikipedia</a>
 * @author Manik Surtani
 * @version 4.1
 */
public class MurmurHash2 {
   private static final int M = 0x5bd1e995;
   private static final int R = 24;
   private static final int H = -1;
   
   /**
    * Hashes a byte array efficiently.
    * @param payload a byte array to hash
    * @return a hash code for the byte array
    */
   public static final int hash(byte[] payload) {
      int h = H;
      int len = payload.length;
      int offset = 0;
      while (len >= 4) {
         int k = payload[offset];
         k |= payload[offset + 1] << 8;
         k |= payload[offset + 2] << 16;
         k |= payload[offset + 3] << 24;

         k *= M;
         k ^= k >> R;
         k *= M;
         h *= M;
         h ^= k;

         len -= 4;
         offset += 4;
      }

      switch (len) {
         case 3:
            h ^= payload[offset + 2] << 16;
         case 2:
            h ^= payload[offset + 1] << 8;
         case 1:
            h ^= payload[offset];
            h *= M;
      }

      h ^= h >> 13;
      h *= M;
      h ^= h >> 15;

      return h;
   }

   /**
    * An incremental version of the hash function, that spreads a pre-calculated hash code, such as one derived from
    * {@link Object#hashCode()}.
    * @param hashcode an object's hashcode
    * @return a spread and hashed version of the hashcode
    */
   public static final int hash(int hashcode) {
      byte[] b = new byte[4];
      b[0] = (byte) hashcode;
      b[1] = (byte) (hashcode >> 8);
      b[2] = (byte) (hashcode >> 16);
      b[3] = (byte) (hashcode >> 24);
      return hash(b);
   }

   /**
    * A helper that calculates the hashcode of an object, choosing the optimal mechanism of hash calculation after
    * considering the type of the object (byte array, String or Object).
    * @param o object to hash
    * @return a hashcode
    */
   public static final int hash(Object o) {
      if (o instanceof byte[])
         return hash((byte[]) o);
      else if (o instanceof String)
         return hash(((String) o).getBytes());
      else if (o instanceof ByteArrayKey)
         return hash(((ByteArrayKey) o).getData());
      else
         return hash(o.hashCode());
   }
}
