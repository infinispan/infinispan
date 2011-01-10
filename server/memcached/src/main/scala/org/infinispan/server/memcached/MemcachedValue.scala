package org.infinispan.server.memcached

import org.infinispan.server.core.CacheValue
import org.infinispan.util.Util
import java.io.{ObjectOutput, ObjectInput}
import org.infinispan.marshall.AbstractExternalizer
import scala.collection.JavaConversions._

/**
 * Memcached value part of key/value pair containing flags on top the common byte array and version.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class MemcachedValue(override val data: Array[Byte], override val version: Long, val flags: Long)
      extends CacheValue(data, version) {

   override def toString = {
      new StringBuilder().append("MemcachedValue").append("{")
         .append("data=").append(Util.printArray(data, false))
         .append(", version=").append(version)
         .append(", flags=").append(flags)
         .append("}").toString
   }

}

object MemcachedValue {
   class Externalizer extends AbstractExternalizer[MemcachedValue] {
      override def writeObject(output: ObjectOutput, cacheValue: MemcachedValue) {
         output.writeInt(cacheValue.data.length)
         output.write(cacheValue.data)
         output.writeLong(cacheValue.version)
         output.writeLong(cacheValue.flags)
      }

      override def readObject(input: ObjectInput): MemcachedValue = {
         val data = new Array[Byte](input.readInt())
         input.readFully(data)
         val version = input.readLong
         val flags = input.readLong
         new MemcachedValue(data, version, flags)
      }

      override def getTypeClasses =
         asJavaSet(Set[java.lang.Class[_ <: MemcachedValue]](classOf[MemcachedValue]))
   }
}