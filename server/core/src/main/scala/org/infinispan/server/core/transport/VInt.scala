package org.infinispan.server.core.transport

import java.lang.IllegalStateException
import io.netty.buffer.ByteBuf
import org.infinispan.server.core.logging.Log

/**
 * Reads and writes unsigned variable length integer values. Even though it's deprecated, do not
 * remove from source code for the moment because it's a good scala example and could be used
 * as reference. 
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object VInt extends Log {

   def write(out: ByteBuf, i: Int) {
      if ((i & ~0x7F) == 0) out.writeByte(i.toByte)
      else {
         out.writeByte(((i & 0x7f) | 0x80).toByte)
         write(out, i >>> 7)
      }
   }

   def read(in: ByteBuf): Int = {
      val b = in.readByte
      trace("Read byte " + b);
      read(in, b, 7, b & 0x7F, 1)
   }

   private def read(in: ByteBuf, b: Byte, shift: Int, i: Int, count: Int): Int = {
      if ((b & 0x80) == 0) i
      else {
         if (count > 5)
            throw new IllegalStateException(
               "Stream corrupted.  A variable length integer cannot be longer than 5 bytes.")

         val bb = in.readByte
         trace("Read byte " + bb);
         read(in, bb, shift + 7, i | ((bb & 0x7FL) << shift).toInt, count + 1)
      }
   }
}