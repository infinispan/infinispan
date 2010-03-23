package org.infinispan.server.core.transport.netty

import org.infinispan.server.core.transport.ChannelBuffer
import org.jboss.netty.buffer.{ChannelBuffer => NettyChannelBuffer}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

class ChannelBufferAdapter(val buffer: NettyChannelBuffer) extends ChannelBuffer {
   
   override def readByte: Byte = buffer.readByte
   override def readBytes(dst: Array[Byte], dstIndex: Int, length: Int) = buffer.readBytes(dst, dstIndex, length)
   override def readUnsignedByte: Float = buffer.readUnsignedByte
   override def readUnsignedInt: Int = { // TODO
      0
      }
   override def readUnsignedLong: Long = { // TODO
      0
      }
   override def readBytes(length: Int): ChannelBuffer = new ChannelBufferAdapter(buffer.readBytes(length))
   override def readerIndex: Int = readerIndex
   override def readBytes(dst: Array[Byte]) = buffer.readBytes(dst) 
   override def readRangedBytes: Array[Byte] = { // TODO
      null
      }
   override def readableBytes = buffer.writerIndex - buffer.readerIndex

   /**
    * Reads length of String and then returns an UTF-8 formatted String of such length.
    */
   override def readString: String = { // TODO
      null
      }
   override def writeByte(value: Byte) = buffer.writeByte(value)
   override def writeBytes(src: Array[Byte]) = buffer.writeBytes(src)

   /**
    * Writes the length of the byte array and transfers the specified source array's data to this buffer
   */
   override def writeRangedBytes(src: Array[Byte]) { // TODO
      0
      }
   override def writeUnsignedInt(i: Int) { // TODO
      }
   override def writeUnsignedLong(l: Long) { // TODO
      }
   override def writerIndex: Int = buffer.writerIndex

   /**
    * Writes the length of the String followed by the String itself. This methods expects String not to be null.
    */
   override def writeString(msg: String) { // TODO
      }

   override def getUnderlyingChannelBuffer: AnyRef = buffer

}