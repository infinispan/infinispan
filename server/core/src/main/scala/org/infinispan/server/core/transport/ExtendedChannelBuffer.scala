package org.infinispan.server.core.transport

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.util.CharsetUtil

object ExtendedChannelBuffer {

   def wrappedBuffer(array: Array[Byte]*) = ChannelBuffers.wrappedBuffer(array : _*)
   def buffer(capacity: Int) = ChannelBuffers.buffer(capacity)
   def dynamicBuffer = ChannelBuffers.dynamicBuffer()

   def readUnsignedShort(bf: ChannelBuffer): Int = bf.readUnsignedShort
   def readUnsignedInt(bf: ChannelBuffer): Int = VInt.read(bf)
   def readUnsignedLong(bf: ChannelBuffer): Long = VLong.read(bf)

   def readRangedBytes(bf: ChannelBuffer): Array[Byte] = {
      val length = readUnsignedInt(bf)
      if (length > 0) {
         val array = new Array[Byte](length)
         bf.readBytes(array)
         array;
      } else {
         Array[Byte]()
      }
   }

   /**
    * Reads length of String and then returns an UTF-8 formatted String of such length.
    * If the length is 0, an empty String is returned.
    */
   def readString(bf: ChannelBuffer): String = {
      val bytes = readRangedBytes(bf)
      if (!bytes.isEmpty) new String(bytes, CharsetUtil.UTF_8) else ""
   }

   def writeUnsignedShort(i: Int, bf: ChannelBuffer) = bf.writeShort(i)
   def writeUnsignedInt(i: Int, bf: ChannelBuffer) = VInt.write(bf, i)
   def writeUnsignedLong(l: Long, bf: ChannelBuffer) = VLong.write(bf, l)

   def writeRangedBytes(src: Array[Byte], bf: ChannelBuffer) {
      writeUnsignedInt(src.length, bf)
      bf.writeBytes(src)
   }

   def writeString(msg: String, bf: ChannelBuffer) = writeRangedBytes(msg.getBytes(CharsetUtil.UTF_8), bf)

}