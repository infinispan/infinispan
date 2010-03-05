package org.infinispan.server.hotrod

import java.util.Arrays

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

final class Key(val k: Array[Byte]) {

   override def equals(obj: Any) = {
      obj match {
         case k: Key => Arrays.equals(k.k, this.k)
         case _ => false
      }
   }

   override def hashCode: Int = 41 + Arrays.hashCode(k)

   override def toString = {
      new StringBuilder().append("Key").append("{")
         .append("k=").append(k)
         .append("}").toString
   }

}