package org.infinispan.server.memcached

import org.infinispan.server.core.transport.ChannelBuffer

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */
// todo: refactor name once old code has been removed?
trait TextProtocolUtil {

   final val CRLF = "\r\n"
   final val CRLFBytes = "\r\n".getBytes
   final val END = "END\r\n".getBytes
   final val DELETED = "DELETED\r\n".getBytes
   final val NOT_FOUND = "NOT_FOUND\r\n".getBytes
   final val EXISTS = "EXISTS\r\n".getBytes
   final val STORED = "STORED\r\n".getBytes
   final val NOT_STORED = "NOT_STORED\r\n".getBytes
   final val OK = "OK\r\n".getBytes
   final val ERROR = "ERROR\r\n".getBytes

   final val CR = 13
   final val LF = 10

   def readElement(buffer: ChannelBuffer): String = readElement(buffer, new StringBuilder())

   private def readElement(buffer: ChannelBuffer, sb: StringBuilder): String = {
      var next = buffer.readByte 
      if (next == 32) { // Space
         sb.toString.trim
      }
      else if (next == 13) { // CR
         next = buffer.readByte
         if (next == 10) { // LF
            sb.toString.trim
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

   def readLine(buffer: ChannelBuffer): String = {
      if (buffer.readableBytes > 0)
         readLine(buffer, new StringBuilder())         
      else
         ""
   }

   private def readLine(buffer: ChannelBuffer, sb: StringBuilder): String = {
      var next = buffer.readByte
      if (next == 13) { // CR
         next = buffer.readByte
         if (next == 10) { // LF
            sb.toString.trim
         } else {
            sb.append(next.asInstanceOf[Char])
            readLine(buffer, sb)
         }
      } else if (next == 10) { //LF
         sb.toString.trim
      } else {
         sb.append(next.asInstanceOf[Char])
         readLine(buffer, sb)
      }
   }

   def concat(a: Array[Byte], b: Array[Byte]): Array[Byte] = {
       val data = new Array[Byte](a.length + b.length)
       Array.copy(a, 0, data, 0, a.length)
       Array.copy(b, 0, data, a.length, b.length)
       return data
   }

}