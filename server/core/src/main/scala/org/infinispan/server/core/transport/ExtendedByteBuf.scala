package org.infinispan.server.core.transport

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.util.CharsetUtil
import org.infinispan.commons.io.SignedNumeric

import scala.annotation.tailrec


object ExtendedByteBuf {
   def wrappedBuffer(array: Array[Byte]*) = Unpooled.wrappedBuffer(array : _*)
   def buffer(capacity: Int) = Unpooled.buffer(capacity)
   def dynamicBuffer = Unpooled.buffer()

   def readUnsignedShort(bf: ByteBuf): Int = bf.readUnsignedShort
   def readUnsignedInt(bf: ByteBuf): Int = VInt.read(bf)
   def readUnsignedLong(bf: ByteBuf): Long = VLong.read(bf)

   def readRangedBytes(bf: ByteBuf): Array[Byte] = {
      val length = readUnsignedInt(bf)
      readRangedBytes(bf, length)
   }

   def readRangedBytes(bf: ByteBuf, length: Int): Array[Byte] = {
      if (length > 0) {
         val array = new Array[Byte](length)
         bf.readBytes(array)
         array
      } else {
         Array.empty
      }
   }

   /**
    * Reads optional range of bytes. Negative lengths are translated to None,
    * 0 length represents empty Array
    */
   def readOptRangedBytes(bf: ByteBuf): Option[Array[Byte]] = {
      val length = SignedNumeric.decode(readUnsignedInt(bf))
      if (length < 0) None else Some(readRangedBytes(bf, length))
   }

   /**
    * Reads an optional String. 0 length is an empty string, negative length
    * is translated to None.
    */
   def readOptString(bf: ByteBuf): Option[String] = {
      val bytes = readOptRangedBytes(bf)
      bytes.map(new String(_, CharsetUtil.UTF_8))
   }

   /**
    * Reads length of String and then returns an UTF-8 formatted String of such length.
    * If the length is 0, an empty String is returned.
    */
   def readString(bf: ByteBuf): String = {
      val bytes = readRangedBytes(bf)
      if (!bytes.isEmpty) new String(bytes, CharsetUtil.UTF_8) else ""
   }

   /**
    * Reads a byte if possible.  If not present the reader index is reset to the last mark.
    * @param bf
    * @return
    */
   def readMaybeByte(bf: ByteBuf): Option[Byte] = {
      if (bf.readableBytes() >= 1) {
         Some(bf.readByte())
      } else {
         bf.resetReaderIndex()
         None
      }
   }

   def readMaybeLong(bf: ByteBuf): Option[Long] = {
      if (bf.readableBytes() < 8) { bf.resetReaderIndex(); None } else Some(bf.readLong())
   }

   /**
    * Reads a variable long if possible.  If not present the reader index is reset to the last mark.
    * @param bf
    * @return
    */
   def readMaybeVLong(bf: ByteBuf): Option[Long] = {
      if (bf.readableBytes() >= 1) {
         val b = bf.readByte
         @tailrec def read(buf: ByteBuf, b: Byte, shift: Int, i: Long, count: Int): Option[Long] = {
            if ((b & 0x80) == 0) Some(i)
            else {
               if (count > 9)
                  throw new IllegalStateException(
                     "Stream corrupted.  A variable length long cannot be longer than 9 bytes.")

               if (buf.readableBytes() >= 1) {
                  val bb = buf.readByte
                  read(buf, bb, shift + 7, i | (bb & 0x7FL) << shift, count + 1)
               } else {
                  buf.resetReaderIndex()
                  None
               }
            }
         }
         read(bf, b, 7, b & 0x7F, 1)
      } else {
         bf.resetReaderIndex()
         None
      }
   }

   /**
    * Reads a variable size int if possible.  If not present the reader index is reset to the last mark.
    * @param bf
    * @return
    */
   def readMaybeVInt(bf: ByteBuf): Option[Int] = {
      if (bf.readableBytes() >= 1) {
         val b = bf.readByte
         @tailrec def read(buf: ByteBuf, b: Byte, shift: Int, i: Int, count: Int): Option[Int] = {
            if ((b & 0x80) == 0) Some(i)
            else {
               if (count > 5)
                  throw new IllegalStateException(
                     "Stream corrupted.  A variable length integer cannot be longer than 5 bytes.")

               if (buf.readableBytes() >= 1) {
                  val bb = buf.readByte
                  read(buf, bb, shift + 7, i | ((bb & 0x7FL) << shift).toInt, count + 1)
               } else {
                  buf.resetReaderIndex()
                  None
               }
            }
         }
         read(bf, b, 7, b & 0x7F, 1)
      } else {
         bf.resetReaderIndex()
         None
      }
   }

   /**
    * Reads a range of bytes if possible.  If not present the reader index is reset to the last mark.
    * @param bf
    * @return
    */
   def readMaybeRangedBytes(bf: ByteBuf): Option[Array[Byte]] = {
      val length = readMaybeVInt(bf)
      if (length.isDefined) {
         if (bf.readableBytes() >= length.get) {
           if (length.get > 0) {
             val array = new Array[Byte](length.get)
             bf.readBytes(array)
             Some(array)
           } else {
             Some(Array[Byte]())
           }
         } else {
           bf.resetReaderIndex()
           None
         }
      } else None
   }

  def readMaybeRangedBytes(bf: ByteBuf, length: Int): Option[Array[Byte]] = {
     if (bf.readableBytes() < length) {
       bf.resetReaderIndex()
       None
     } else {
       val bytes = new Array[Byte](length)
       bf.readBytes(bytes)
       Some(bytes)
     }
  }

  def readMaybeSignedInt(bf: ByteBuf): Option[Int] = readMaybeVInt(bf).map(SignedNumeric.decode)

  def readMaybeOptRangedBytes(bf: ByteBuf): MaybeBytesTrait = {
     val l = readMaybeSignedInt(bf)
     l.map(length => if (length < 0) BytesNotPresent else {
       readMaybeRangedBytes(bf, length).map(b => new PresentBytes(b)).getOrElse(MoreBytesForBytes)
     }).getOrElse(MoreBytesForBytes)
  }

  /**
   * Reads a string if possible.  If not present the reader index is reset to the last mark.
   * @param bf
   * @return
   */
  def readMaybeString(bf: ByteBuf): Option[String] = {
    val bytes = readMaybeRangedBytes(bf)
    bytes.map(b => if (b.isEmpty) "" else new String(b, CharsetUtil.UTF_8))
  }

  def readMaybeOptString(bf: ByteBuf): MaybeStringTrait = {
    readMaybeOptRangedBytes(bf) match {
      case BytesNotPresent => StringNotPresent
      case MoreBytesForBytes => MoreBytesForString
      case p: PresentBytes => new PresentString(if (p.getValue().isEmpty) "" else new Predef.String(p.getValue(), CharsetUtil.UTF_8))
    }
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
      writeRangedBytes(msg.map(_.getBytes(CharsetUtil.UTF_8)).getOrElse(Array.empty), bf)

  sealed trait MaybeStringTrait {
    def getValue(): String = throw new IllegalStateException("Value was not provided")
  }

  case object MoreBytesForString extends MaybeStringTrait
  case object StringNotPresent extends MaybeStringTrait

  final case class PresentString(value: String) extends MaybeStringTrait {
    override def getValue(): String = value
  }

  sealed trait MaybeBytesTrait {
    def getValue(): Array[Byte] = throw new IllegalStateException("Value was not provided")
  }

  final case class PresentBytes(value: Array[Byte]) extends MaybeBytesTrait {
    override def getValue(): Array[Byte] = value
  }

  case object MoreBytesForBytes extends MaybeBytesTrait
  case object BytesNotPresent extends MaybeBytesTrait
}