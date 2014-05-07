package org.infinispan.server.core.transport

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.util.CharsetUtil


object ExtendedByteBuf {

   def wrappedBuffer(array: Array[Byte]*) = Unpooled.wrappedBuffer(array : _*)
   def buffer(capacity: Int) = Unpooled.buffer(capacity)
   def dynamicBuffer = Unpooled.buffer()

   def readUnsignedShort(bf: ByteBuf): Int = bf.readUnsignedShort
   def readUnsignedInt(bf: ByteBuf): Int = VInt.read(bf)
   def readUnsignedLong(bf: ByteBuf): Long = VLong.read(bf)

   def readRangedBytes(bf: ByteBuf): Array[Byte] = {
      val length = readUnsignedInt(bf)
      if (length > 0) {
         val array = new Array[Byte](length)
         bf.readBytes(array)
         array
      } else {
         Array[Byte]()
      }
   }

   /**
    * Reads length of String and then returns an UTF-8 formatted String of such length.
    * If the length is 0, an empty String is returned.
    */
   def readString(bf: ByteBuf): String = {
      val bytes = readRangedBytes(bf)
      if (!bytes.isEmpty) new String(bytes, CharsetUtil.UTF_8) else ""
   }

   def writeUnsignedShort(i: Int, bf: ByteBuf) = bf.writeShort(i)
   def writeUnsignedInt(i: Int, bf: ByteBuf) = VInt.write(bf, i)
   def writeUnsignedLong(l: Long, bf: ByteBuf) = VLong.write(bf, l)

   def writeRangedBytes(src: Array[Byte], bf: ByteBuf) {
      writeUnsignedInt(src.length, bf)
      if (src.length > 0)
         bf.writeBytes(src)
   }

   def writeString(msg: String, bf: ByteBuf) = writeRangedBytes(msg.getBytes(CharsetUtil.UTF_8), bf)

   def writeString(msg: Option[String], bf: ByteBuf) =
      writeRangedBytes(msg.map(_.getBytes(CharsetUtil.UTF_8)).getOrElse(Array()), bf)

}