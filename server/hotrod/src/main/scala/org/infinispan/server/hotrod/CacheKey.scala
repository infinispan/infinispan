package org.infinispan.server.hotrod

import org.infinispan.util.Util
import java.util.Arrays
import org.infinispan.marshall.{Externalizer, Marshallable}
import java.io.{ObjectInput, ObjectOutput}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */
// TODO: putting Ids.HOTROD_CACHE_KEY fails compilation in 2.8 - https://lampsvn.epfl.ch/trac/scala/ticket/2764
@Marshallable(externalizer = classOf[CacheKeyExternalizer], id = 57)
final class CacheKey(val data: Array[Byte]) {

   override def equals(obj: Any) = {
      obj match {
         // Apparenlty this is the way arrays should be compared for equality of contents, see:
         // http://old.nabble.com/-scala--Array-equality-td23149094.html
         case k: CacheKey => Arrays.equals(k.data, this.data)
         case _ => false
      }
   }

   override def hashCode: Int = 41 + Arrays.hashCode(data)

   override def toString = {
      new StringBuilder().append("CacheKey").append("{")
         .append("data=").append(Util.printArray(data, true))
         .append("}").toString
   }

}

private class CacheKeyExternalizer extends Externalizer {
   override def writeObject(output: ObjectOutput, obj: AnyRef) {
      val cacheKey = obj.asInstanceOf[CacheKey]
      output.write(cacheKey.data.length)
      output.write(cacheKey.data)
   }

   override def readObject(input: ObjectInput): AnyRef = {
      val data = new Array[Byte](input.read())
      input.read(data)
      new CacheKey(data)
   }
}