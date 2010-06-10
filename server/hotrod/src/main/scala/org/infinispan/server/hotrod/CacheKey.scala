package org.infinispan.server.hotrod

import org.infinispan.util.Util
import java.util.Arrays
import org.infinispan.marshall.Marshallable
import java.io.{ObjectInput, ObjectOutput}
import org.infinispan.server.core.Logging
import org.infinispan.util.hash.MurmurHash2

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */
// TODO: putting Ids.HOTROD_CACHE_KEY fails compilation in 2.8 - https://lampsvn.epfl.ch/trac/scala/ticket/2764
@Marshallable(externalizer = classOf[CacheKey.Externalizer], id = 57)
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
//      MurmurHash2.hash(data)
   }

   override def toString = {
      new StringBuilder().append("CacheKey").append("{")
         .append("data=").append(Util.printArray(data, true))
         .append("}").toString
   }

}

object CacheKey extends Logging {
   class Externalizer extends org.infinispan.marshall.Externalizer {
      override def writeObject(output: ObjectOutput, obj: AnyRef) {
         val cacheKey = obj.asInstanceOf[CacheKey]
         output.writeInt(cacheKey.data.length)
         output.write(cacheKey.data)
      }

      override def readObject(input: ObjectInput): AnyRef = {
         val data = new Array[Byte](input.readInt())
         input.readFully(data)
         new CacheKey(data)
      }
   }
}
