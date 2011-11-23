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
package org.infinispan.commons.hash;

import org.infinispan.marshall.Ids;
import org.infinispan.marshall.exts.NoStateExternalizer;
import org.infinispan.util.ByteArrayKey;
import org.infinispan.util.Util;

import java.io.ObjectInput;
import java.util.Set;

/**
 * An implementation of Austin Appleby's MurmurHash2.0 algorithm, as documented on <a href="http://sites.google.com/site/murmurhash/">his website</a>.
 * <p />
 * This implementation is based on the slower, endian-neutral version of the algorithm as documented on the site,
 * ported from Austin Appleby's original C++ version <a href="http://sites.google.com/site/murmurhash/MurmurHashNeutral2.cpp">MurmurHashNeutral2.cpp</a>.
 * <p />
 * Other implementations are documented on Wikipedia's <a href="http://en.wikipedia.org/wiki/MurmurHash">MurmurHash</a> page.
 * <p />
 * <b>Note</b>: This is the backward compatible version of this hash.  The correct version of this hash is implemented as
 * {@link MurmurHash2}.  This version contains a slight bug in that it only takes into account 31 bits of the 32 bit range,
 * which will result in poorer distribution.  It is still maintained in the source tree to provide backward compatibility
 * with existing clusters prior to 4.2.1, when the bug was identified and fixed.  See <a href="https://issues.jboss.org/browse/ISPN-873">ISPN-873</a> for more details.
 *
 * @see <a href="http://sites.google.com/site/murmurhash/">MurmurHash website</a>
 * @see <a href="http://en.wikipedia.org/wiki/MurmurHash">MurmurHash entry on Wikipedia</a>
 * @see MurmurHash2
 * @author Manik Surtani
 * @version 4.1
 */
public class MurmurHash2Compat implements Hash {
   private static final int M = 0x5bd1e995;
   private static final int R = 24;
   private static final int H = -1;

   public final int hash(byte[] payload) {
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

   public final int hash(int hashcode) {
      byte[] b = new byte[4];
      b[0] = (byte) hashcode;
      b[1] = (byte) (hashcode >> 8);
      b[2] = (byte) (hashcode >> 16);
      b[3] = (byte) (hashcode >> 24);
      return hash(b);
   }

   public final int hash(Object o) {
      if (o instanceof byte[])
         return hash((byte[]) o);
      else if (o instanceof String)
         return hash(((String) o).getBytes());
      else if (o instanceof ByteArrayKey)
         return hash(((ByteArrayKey) o).getData());
      else
         return hash(o.hashCode());
   }

   public static class Externalizer extends NoStateExternalizer<MurmurHash2Compat> {
      @Override
      public Set<Class<? extends MurmurHash2Compat>> getTypeClasses() {
         return Util.<Class<? extends MurmurHash2Compat>>asSet(MurmurHash2Compat.class);
      }

      @Override
      public MurmurHash2Compat readObject(ObjectInput input) {
         return new MurmurHash2Compat();
      }

      @Override
      public Integer getId() {
         return Ids.MURMURHASH_2_COMPAT;
      }
   }
}
