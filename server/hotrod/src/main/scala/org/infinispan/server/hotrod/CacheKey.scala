package org.infinispan.server.hotrod

import java.io.Serializable
import org.infinispan.util.Util
import java.util.Arrays

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

final class CacheKey(val k: Array[Byte]) extends Serializable {

   override def equals(obj: Any) = {
      obj match {
         // TODO: find out the right way to compare arrays in Scala
         case k: CacheKey => Arrays.equals(k.k, this.k)
         case _ => false
      }
   }

   override def hashCode: Int = 41 + Arrays.hashCode(k)

   override def toString = {
      new StringBuilder().append("HotRodKey").append("{")
         .append("k=").append(Util.printArray(k, true))
         .append("}").toString
   }

}