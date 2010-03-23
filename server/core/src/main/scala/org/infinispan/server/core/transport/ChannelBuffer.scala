package org.infinispan.server.core.transport

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

abstract class ChannelBuffer {
   def readByte: Byte
   def readBytes(dst: Array[Byte], dstIndex: Int, length: Int)
   def readUnsignedByte: Float
   def readUnsignedInt: Int
   def readUnsignedLong: Long
   def readBytes(length: Int): ChannelBuffer
   def readerIndex: Int
   def readBytes(dst: Array[Byte]): Unit
   def readRangedBytes: Array[Byte]
   def readableBytes: Int

   /**
    * Reads length of String and then returns an UTF-8 formatted String of such length.
    */
   def readString: String
   def writeByte(value: Byte)
   def writeBytes(src: Array[Byte])

   /**
    * Writes the length of the byte array and transfers the specified source array's data to this buffer
   */
   def writeRangedBytes(src: Array[Byte])
   def writeUnsignedInt(i: Int)
   def writeUnsignedLong(l: Long)
   def writerIndex: Int

   /**
    * Writes the length of the String followed by the String itself. This methods expects String not to be null.
    */
   def writeString(msg: String)

   def getUnderlyingChannelBuffer: AnyRef
}