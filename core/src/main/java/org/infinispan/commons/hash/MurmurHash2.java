/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
import java.nio.charset.Charset;
import java.util.Set;

/**
 * An implementation of Austin Appleby's MurmurHash2.0 algorithm, as documented on <a href="http://sites.google.com/site/murmurhash/">his website</a>.
 * <p />
 * This implementation is based on the slower, endian-neutral version of the algorithm as documented on the site,
 * ported from Austin Appleby's original C++ version <a href="http://sites.google.com/site/murmurhash/MurmurHashNeutral2.cpp">MurmurHashNeutral2.cpp</a>.
 * <p />
 * Other implementations are documented on Wikipedia's <a href="http://en.wikipedia.org/wiki/MurmurHash">MurmurHash</a> page.
 * <p />
 * @see <a href="http://sites.google.com/site/murmurhash/">MurmurHash website</a>
 * @see <a href="http://en.wikipedia.org/wiki/MurmurHash">MurmurHash entry on Wikipedia</a>
 * @see MurmurHash2Compat
 * @author Manik Surtani
 * @version 4.1
 */
public class MurmurHash2 implements Hash {
   private static final int M = 0x5bd1e995;
   private static final int R = 24;
   private static final int H = -1;
   private static final Charset UTF8 = Charset.forName("UTF-8");

   @Override
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
         k ^= k >>> R;
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

      h ^= h >>> 13;
      h *= M;
      h ^= h >>> 15;

      return h;
   }

   @Override
   public final int hash(int hashcode) {
      byte[] b = new byte[4];
      b[0] = (byte) hashcode;
      b[1] = (byte) (hashcode >>> 8);
      b[2] = (byte) (hashcode >>> 16);
      b[3] = (byte) (hashcode >>> 24);
      return hash(b);
   }

   @Override
   public final int hash(Object o) {
      if (o instanceof byte[])
         return hash((byte[]) o);
      else if (o instanceof String)
         return hash(((String) o).getBytes(UTF8));
      else if (o instanceof ByteArrayKey)
         return hash(((ByteArrayKey) o).getData());
      else
         return hash(o.hashCode());
   }

   public boolean equals(Object other) {
      return other != null && other.getClass() == getClass();
   }

   public static class Externalizer extends NoStateExternalizer<MurmurHash2> {
      @Override
      public Set<Class<? extends MurmurHash2>> getTypeClasses() {
         return Util.<Class<? extends MurmurHash2>>asSet(MurmurHash2.class);
      }

      @Override
      public MurmurHash2 readObject(ObjectInput input) {
         return new MurmurHash2();
      }

      @Override
      public Integer getId() {
         return Ids.MURMURHASH_2;
      }
   }
}
