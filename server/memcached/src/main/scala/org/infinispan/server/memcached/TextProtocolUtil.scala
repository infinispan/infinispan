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
   final val CR = 13
   final val LF = 10

   // TODO: maybe convert to recursive
   def readElement(buffer: ChannelBuffer): String = {
      if (buffer.readableBytes > 0)
         readElement(buffer, new StringBuilder())
      else
         ""
   }

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