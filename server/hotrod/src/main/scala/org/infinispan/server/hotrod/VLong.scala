package org.infinispan.server.hotrod

import org.infinispan.server.core.transport.ChannelBuffer

/**
 * Reads and writes unsigned variable length long values. Even though it's deprecated, do not
 * remove from source code for the moment because it's a good scala example and could be used
 * as reference.
 *
 * @author Galder Zamarreño
 * @since 4.1
 * @deprecated Instead use ChannelBuffer.writeUnsignedLong and ChannelBuffer.readUnsignedLong
 */
@deprecated("Instead use ChannelBuffer.writeUnsignedLong and ChannelBuffer.readUnsignedLong")
object VLong {

   def write(out: ChannelBuffer, i: Long) {
      if ((i & ~0x7F) == 0) out.writeByte(i.toByte)
      else {
         out.writeByte(((i & 0x7f) | 0x80).toByte)
         write(out, i >>> 7)
      }
   }

   def read(in: ChannelBuffer): Long = {
      val b = in.readByte
      read(in, b, 7, b & 0x7F)
   }

   private def read(in: ChannelBuffer, b: Byte, shift: Int, i: Long): Long = {
      if ((b & 0x80) == 0) i
      else {
         val bb = in.readByte
         read(in, bb, shift + 7, i | (bb & 0x7FL) << shift)
      }
   }
}