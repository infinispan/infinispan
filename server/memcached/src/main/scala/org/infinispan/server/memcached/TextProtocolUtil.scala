package org.infinispan.server.memcached

import org.infinispan.server.core.transport.ChannelBuffer

/**
 * Memcached text protocol utilities.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
trait TextProtocolUtil {
   // todo: refactor name once old code has been removed?

   val CRLF = "\r\n"
   val CRLFBytes = "\r\n".getBytes
   val END = "END\r\n".getBytes
   val DELETED = "DELETED\r\n".getBytes
   val NOT_FOUND = "NOT_FOUND\r\n".getBytes
   val EXISTS = "EXISTS\r\n".getBytes
   val STORED = "STORED\r\n".getBytes
   val NOT_STORED = "NOT_STORED\r\n".getBytes
   val OK = "OK\r\n".getBytes
   val ERROR = "ERROR\r\n".getBytes
   val CLIENT_ERROR_BAD_FORMAT = "CLIENT_ERROR bad command line format: "
   val SERVER_ERROR = "SERVER_ERROR "

   val CR = 13
   val LF = 10

   val MAX_UNSIGNED_LONG = BigInt("18446744073709551615")
   val MIN_UNSIGNED = BigInt("0")

   /**
    * In the particular case of Memcached, the end of operation/command
    * is signaled by "\r\n" characters. So, if end of operation is
    * found, this method would return the element and true. On the
    * contrary, if space was found instead of end of operation
    * character, then it'd return the element and false.
    */
   def readElement(buffer: ChannelBuffer): (String, Boolean) = readElement(buffer, new StringBuilder())

   private def readElement(buffer: ChannelBuffer, sb: StringBuilder): (String, Boolean) = {
      var next = buffer.readByte 
      if (next == 32) { // Space
         (sb.toString.trim, false)
      }
      else if (next == 13) { // CR
         next = buffer.readByte
         if (next == 10) { // LF
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