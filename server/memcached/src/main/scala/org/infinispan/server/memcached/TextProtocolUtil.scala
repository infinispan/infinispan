package org.infinispan.server.memcached

import java.lang.StringBuilder
import collection.mutable.{Buffer, ListBuffer}
import annotation.tailrec
import java.io.{ByteArrayOutputStream, OutputStream}
import io.netty.buffer.ByteBuf

/**
 * Memcached text protocol utilities.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object TextProtocolUtil {
   // todo: refactor name once old code has been removed?

   val CRLF = "\r\n"
   val CRLFBytes = "\r\n".getBytes
   val END = "END\r\n".getBytes
   val END_SIZE = END.length
   val DELETED = "DELETED\r\n".getBytes
   val NOT_FOUND = "NOT_FOUND\r\n".getBytes
   val EXISTS = "EXISTS\r\n".getBytes
   val STORED = "STORED\r\n".getBytes
   val NOT_STORED = "NOT_STORED\r\n".getBytes
   val OK = "OK\r\n".getBytes
   val ERROR = "ERROR\r\n".getBytes
   val CLIENT_ERROR_BAD_FORMAT = "CLIENT_ERROR bad command line format: "
   val SERVER_ERROR = "SERVER_ERROR "
   val VALUE = "VALUE ".getBytes
   val VALUE_SIZE = VALUE.length
   val ZERO = "0".getBytes

   val SP = 32
   val CR = 13
   val LF = 10

   val MAX_UNSIGNED_LONG = BigInt("18446744073709551615")
   val MIN_UNSIGNED = BigInt("0")

   val CHARSET = "UTF-8"

   /**
    * In the particular case of Memcached, the end of operation/command
    * is signaled by "\r\n" characters. So, if end of operation is
    * found, this method would return true. On the contrary, if space was
    * found instead of end of operation character, then it'd return the element and false.
    */
   @tailrec
   def readElement(buffer: ByteBuf, byteBuffer: OutputStream): Boolean = {
      var next = buffer.readByte
      if (next == SP) { // Space
         false
      }
      else if (next == CR) { // CR
         next = buffer.readByte
         if (next == LF) { // LF
            true
         } else {
            byteBuffer.write(next)
            readElement(buffer, byteBuffer)
         }
      }
      else {
         byteBuffer.write(next)
         readElement(buffer, byteBuffer)
      }
   }

   def extractString(byteBuffer: ByteArrayOutputStream): String = {
      val string = new String(byteBuffer.toByteArray(), CHARSET);
      byteBuffer.reset()
      string
   }

   @tailrec
   private def readElement(buffer: ByteBuf, sb: StringBuilder): (String, Boolean) = {
      var next = buffer.readByte 
      if (next == SP) { // Space
         (sb.toString.trim, false)
      }
      else if (next == CR) { // CR
         next = buffer.readByte
         if (next == LF) { // LF
            (sb.toString.trim, true)
         } else {
            sb.append(next.asInstanceOf[Char])
            readElement(buffer, sb)
         }
      }
      else {
         sb.append(next.asInstanceOf[Char])
         readElement(buffer, sb)
      }
   }

   def readDiscardedLine(buffer: ByteBuf): String = {
      if (readableBytes(buffer) > 0)
         readDiscardedLine(buffer, new StringBuilder())
      else
         ""
   }

   def readableBytes(buffer: ByteBuf): Int =
      buffer.writerIndex - buffer.readerIndex

   @tailrec
   private def readDiscardedLine(buffer: ByteBuf, sb: StringBuilder): String = {
      var next = buffer.readByte
      if (next == CR) { // CR
         next = buffer.readByte
         if (next == LF) { // LF
            sb.toString.trim
         } else {
            sb.append(next.asInstanceOf[Char])
            readDiscardedLine(buffer, sb)
         }
      } else if (next == LF) { //LF
         sb.toString.trim
      } else {
         sb.append(next.asInstanceOf[Char])
         readDiscardedLine(buffer, sb)
      }
   }

   @tailrec
   def skipLine(buffer: ByteBuf) {
      if (readableBytes(buffer) > 0) {
         var next = buffer.readByte
         if (next == CR) { // CR
            next = buffer.readByte
            if (next == LF) { // LF
               return
            } else {
               skipLine(buffer)
            }
         } else if (next == LF) { //LF
            return
         } else {
            skipLine(buffer)
         }
      }
   }

   def concat(a: Array[Byte], b: Array[Byte]): Array[Byte] = {
       val data = new Array[Byte](a.length + b.length)
       Array.copy(a, 0, data, 0, a.length)
       Array.copy(b, 0, data, a.length, b.length)
       data
   }

   def readSplitLine(buffer: ByteBuf): Seq[String] = {
      if (readableBytes(buffer) > 0)
         readSplitLine(buffer, new ListBuffer[String](), buffer.readerIndex(), 0)
      else
         Seq.empty
   }

   @tailrec
   private def readSplitLine(buffer: ByteBuf, list: Buffer[String],
           start: Int, length: Int): Seq[String] = {
      var next = buffer.readByte
      if (next == CR) { // CR
         next = buffer.readByte
         if (next == LF) { // LF
            val bytes = new Array[Byte](length)
            buffer.getBytes(start, bytes)
            list += new String(bytes, CHARSET)
            Seq[String]() ++ list
         } else {
            readSplitLine(buffer, list, start, length + 1)
         }
      } else if (next == LF) { // LF
         val bytes = new Array[Byte](length)
         buffer.getBytes(start, bytes)
         list += new String(bytes, CHARSET)
         Seq[String]() ++ list
      } else if (next == SP) {
         val bytes = new Array[Byte](length)
         buffer.getBytes(start, bytes)
         list += new String(bytes, CHARSET)
         readSplitLine(buffer, list, start + length + 1, 0)
      } else {
         readSplitLine(buffer, list, start, length + 1)
      }
   }

}