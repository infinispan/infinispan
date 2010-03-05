package org.infinispan.server.hotrod

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

final class Value(val v: Array[Byte]) {

   override def toString = {
      new StringBuilder().append("Value").append("{")
         .append("v=").append(v)
         .append("}").toString
   }

}