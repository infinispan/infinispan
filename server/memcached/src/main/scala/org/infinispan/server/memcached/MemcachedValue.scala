package org.infinispan.server.memcached

import org.infinispan.server.core.CacheValue
import org.infinispan.commons.util.Util
import java.io.{ObjectOutput, ObjectInput}
import org.infinispan.commons.marshall.AbstractExternalizer
import scala.collection.JavaConversions._
import java.lang.StringBuilder

/**
 * Memcached value part of key/value pair containing flags on top the common
 * byte array and version.
 *
 * The class can be marshalled either via its externalizer or via the JVM
 * serialization.  The reason for supporting both methods is to enable
 * third-party libraries to be able to marshall/unmarshall them using standard
 * JVM serialization rules.  The Infinispan marshalling layer will always
 * chose the most performant one, aka the AdvancedExternalizer method.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
// TODO: Create a Metadata extension for Memcached value and add flags, removing this class
class MemcachedValue(override val data: Array[Byte], override val version: Long, val flags: Long)
      extends CacheValue(data, version) with Serializable {

   override def toString = {
      new StringBuilder().append("MemcachedValue").append("{")
         .append("data=").append(Util.toStr(data))
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
         setAsJavaSet(Set[java.lang.Class[_ <: MemcachedValue]](classOf[MemcachedValue]))
   }
}
