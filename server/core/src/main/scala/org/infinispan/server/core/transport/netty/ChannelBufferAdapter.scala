package org.infinispan.server.core.transport.netty

import org.jboss.netty.buffer.{ChannelBuffer => NettyChannelBuffer}
import org.infinispan.server.core.transport.{VLong, VInt, ChannelBuffer}
import org.infinispan.server.core.Logging

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class ChannelBufferAdapter(buffer: NettyChannelBuffer) extends ChannelBuffer {
   
   override def readByte: Byte = buffer.readByte
   override def readBytes(dst: Array[Byte], dstIndex: Int, length: Int) = buffer.readBytes(dst, dstIndex, length)
   override def readUnsignedByte: Short = buffer.readUnsignedByte
   override def readUnsignedInt: Int = VInt.read(this)
   override def readUnsignedLong: Long = VLong.read(this)
   override def readBytes(length: Int): ChannelBuffer = new ChannelBufferAdapter(buffer.readBytes(length))
   override def readerIndex: Int = readerIndex
   override def readBytes(dst: Array[Byte]) = buffer.readBytes(dst) 
   override def readRangedBytes: Array[Byte] = {
      val array = new Array[Byte](readUnsignedInt)
      readBytes(array)
      array;
   }
   override def readableBytes = buffer.writerIndex - buffer.readerIndex

   /**
    * Reads length of String and then returns an UTF-8 formatted String of such length.
    */
   override def readString: String = new String(readRangedBytes, "UTF8")
   override def readLong: Long = buffer.readLong
   override def writeByte(value: Byte) = buffer.writeByte(value)
   override def writeBytes(src: Array[Byte]) = buffer.writeBytes(src)

   /**
    * Writes the length of the byte array and transfers the specified source array's data to this buffer
   */
   override def writeRangedBytes(src: Array[Byte]) {
      writeUnsignedInt(src.length)
      writeBytes(src)
   }
   override def writeUnsignedInt(i: Int) = VInt.write(this, i)
   override def writeUnsignedLong(l: Long) = VLong.write(this, l)
   override def writerIndex: Int = buffer.writerIndex

   /**
    * Writes the length of the String followed by the String itself. This methods expects String not to be null.
    */
   override def writeString(msg: String) = writeRangedBytes(msg.getBytes())
   override def writeLong(l: Long) = buffer.writeLong(l)

   override def getUnderlyingChannelBuffer: AnyRef = buffer

}

object ChannelBufferAdapter extends Logging