package org.infinispan.server.hotrod

import org.infinispan.util.Util
import java.util.Arrays
import org.infinispan.marshall.Marshalls
import java.io.{ObjectInput, ObjectOutput}
import org.infinispan.server.core.Logging

/**
 * Represents the key part of a key/value pair stored in the underlying Hot Rod cache.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
final class CacheKey(val data: Array[Byte]) {

   override def equals(obj: Any) = {
      obj match {
         // Apparenlty this is the way arrays should be compared for equality of contents, see:
         // http://old.nabble.com/-scala--Array-equality-td23149094.html
         case k: CacheKey => Arrays.equals(k.data, this.data)
         case _ => false
      }
   }

   override def hashCode: Int = {
      41 + Arrays.hashCode(data)
   }

   override def toString = {
      new StringBuilder().append("CacheKey").append("{")
         .append("data=").append(Util.printArray(data, true))
         .append("}").toString
   }

}

object CacheKey extends Logging {
   // TODO: putting Ids.HOTROD_CACHE_KEY fails compilation in 2.8 - https://lampsvn.epfl.ch/trac/scala/ticket/2764
   @Marshalls(typeClasses = Array(classOf[CacheKey]), id = 57)
   class Externalizer extends org.infinispan.marshall.Externalizer[CacheKey] {
      override def writeObject(output: ObjectOutput, cacheKey: CacheKey) {
         output.writeInt(cacheKey.data.length)
         output.write(cacheKey.data)
      }

      override def readObject(input: ObjectInput): CacheKey = {
         val data = new Array[Byte](input.readInt())
         input.readFully(data)
         new CacheKey(data)
      }
   }
}
