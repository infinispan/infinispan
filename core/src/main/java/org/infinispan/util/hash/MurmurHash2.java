package org.infinispan.util.hash;

import java.util.Random;

/**
 * TODO: Document this
 *
 * @author Manik Surtani
 * @version 4.1
 */
public class MurmurHash2 {

   public static final int hash(byte[] payload) {
      int m = 0x5bd1e995;
      int r = 24;
      int h = new Random().nextInt() ^ 1024;

      int len = payload.length;
      int offset = 0;
      while (len >= 4) {
         int k = payload[offset];
         k |= payload[offset + 1] << 8;
         k |= payload[offset + 2] << 16;
         k |= payload[offset + 3] << 24;

         k *= m;
         k ^= k >> r;
         k *= m;
         h *= m;
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
            h *= m;
      }

      h ^= h >> 13;
      h *= m;
      h ^= h >> 15;

      return h;
   }

   public static final int hash(int hashcode) {
      byte[] b = new byte[4];
      b[0] = (byte) hashcode;
      b[1] = (byte) (hashcode >> 8);
      b[2] = (byte) (hashcode >> 16);
      b[3] = (byte) (hashcode >> 24);
      return hash(b);
   }

   public static final int hash(Object o) {
      if (o instanceof byte[])
         return hash((byte[]) o);
      else if (o instanceof String)
         return hash(((String) o).getBytes());
      else
         return hash(o.hashCode());
   }


}
