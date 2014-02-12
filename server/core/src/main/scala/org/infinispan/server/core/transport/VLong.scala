package org.infinispan.server.core.transport

import io.netty.buffer.ByteBuf

/**
 * Reads and writes unsigned variable length long values. Even though it's deprecated, do not
 * remove from source code for the moment because it's a good scala example and could be used
 * as reference.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object VLong {

   def write(out: ByteBuf, i: Long) {
      if ((i & ~0x7F) == 0) out.writeByte(i.toByte)
      else {
         out.writeByte(((i & 0x7f) | 0x80).toByte)
         write(out, i >>> 7)
      }
   }

   def read(in: ByteBuf): Long = {
      val b = in.readByte
      read(in, b, 7, b & 0x7F, 1)
   }

   private def read(in: ByteBuf, b: Byte, shift: Int, i: Long, count: Int): Long = {
      if ((b & 0x80) == 0) i
      else {
         if (count > 9)
            throw new IllegalStateException(
               "Stream corrupted.  A variable length long cannot be longer than 9 bytes.")

         val bb = in.readByte
         read(in, bb, shift + 7, i | (bb & 0x7FL) << shift, count + 1)
      }
   }
}