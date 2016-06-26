package org.infinispan.server.core.transport;

import io.netty.buffer.ByteBuf;

/**
 * Reads and writes unsigned variable length long values. Even though it's deprecated, do not
 * remove from source code for the moment because it's a good scala example and could be used
 * as reference.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class VLong {

   public static void write(ByteBuf out, long i) {
      if ((i & ~0x7F) == 0) out.writeByte((byte) i);
      else {
         out.writeByte((byte) ((i & 0x7f) | 0x80));
         write(out, i >>> 7);
      }
   }

   public static long read(ByteBuf in) {
      byte b = in.readByte();
      return read(in, b, 7, b & 0x7F, 1);
   }

   private static long read(ByteBuf in, byte b, int shift, long i, int count) {
      if ((b & 0x80) == 0) return i;
      else {
         if (count > 9)
            throw new IllegalStateException(
               "Stream corrupted.  A variable length long cannot be longer than 9 bytes.");

         byte bb = in.readByte();
         return read(in, bb, shift + 7, i | (bb & 0x7FL) << shift, count + 1);
      }
   }
}