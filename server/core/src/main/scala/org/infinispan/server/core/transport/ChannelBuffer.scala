package org.infinispan.server.core.transport

/**
 * A channel buffer to which data can be written and from which data can be read.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class ChannelBuffer {

   /**
    * Read a byte.
    */
   def readByte: Byte

   /**
    * Read bytes from the buffer into the destination byte array.
    *
    * @param dst the destination array byte to which to write read data.
    * @param dstIndex starting index in destination array where to write data.
    * @length number of bytes to read from the buffer. The destination array must have space to hold all these bytes.
    */
   def readBytes(dst: Array[Byte], dstIndex: Int, length: Int)

   /**
    * Read an unsigned Byte from the buffer.
    *
    * @return a Short containing the unsigned Byte that can accommodate top end values that a signed Byte cannot do.
    */
   def readUnsignedByte: Short

   /**
    * Read an unsigned, variable length, Int from the buffer.
    */
   def readUnsignedInt: Int

   /**
    * Read an unsigned, variable length, Long from the buffer.
    */
   def readUnsignedLong: Long

   /**
    * Read an unsigned Short from the buffer.
    *
    * @return an Int containing the unsigned Short that can accommodate top end values that a signed Short cannot do.
    */
   def readUnsignedShort: Int

   /**
    * Returns a ChannelBuffer containing a number of bytes read from the current buffer.
    */
   def readBytes(length: Int): ChannelBuffer

   /**
    * Returns the reader index.
    */
   def readerIndex: Int

   /**
    * Read bytes from the buffer into the destination byte array. The amount of bytes to read are defined
    * by the array's length.
    */
   def readBytes(dst: Array[Byte]): Unit

   /**
    * Reads a ranged number of bytes into a byte array. The number of bytes to be read is defined by an unsigned,
    * variable length, integer that's read from the buffer.
    */
   def readRangedBytes: Array[Byte]

   /**
    * Returns the amount of readable bytes in the buffer.
    */
   def readableBytes: Int

   /**
    * Reads the length of String and then returns an UTF-8 formatted String of such length.
    */
   def readString: String

   /**
    * Read a Long from the buffer.
    */
   def readLong: Long

   /**
    * Read an Int from the buffer.
    */
   def readInt: Int

   /**
    * Write a Byte to the buffer.
    */
   def writeByte(value: Byte)

   /**
    * Write a Byte array to the buffer.
    */
   def writeBytes(src: Array[Byte])

   /**
    * Writes the length of the byte array as an unsigned integer and and then transfers the specified source array's
    * data to this buffer.
    */
   def writeRangedBytes(src: Array[Byte])

   /**
    * Writes an Int as an unsigned, variable length, integer to the buffer.
    */
   def writeUnsignedInt(i: Int)

   /**
    * Writes a Long as an unsigned, variable length, long to the buffer.
    */
   def writeUnsignedLong(l: Long)

   /**
    * Writes an unsigned short to the buffer.
    */
   def writeUnsignedShort(i: Int)

   /**
    * Returns the writer index.
    */
   def writerIndex: Int

   /**
    * Writes the length of the String followed by the String itself. This methods expects String not to be null.
    */
   def writeString(msg: String)

   /**
    * Writes a Long to the buffer.
    */
   def writeLong(l: Long)

   /**
    * Writes an Int to the buffer.
    */
   def writeInt(i: Int)

   /**
    * Retrieve the underlying buffer.
    */
   def getUnderlyingChannelBuffer: AnyRef
}