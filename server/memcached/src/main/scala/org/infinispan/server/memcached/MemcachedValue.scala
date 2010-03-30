package org.infinispan.server.memcached

import org.infinispan.server.core.CacheValue
import org.infinispan.util.Util
import java.io.{ObjectOutput, ObjectInput}
import org.infinispan.marshall.{Marshallable, Externalizer}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */
// TODO: putting Ids.MEMCACHED_CACHE_VALUE fails compilation in 2.8
@Marshallable(externalizer = classOf[MemcachedValueExternalizer], id = 56)
class MemcachedValue(override val data: Array[Byte], override val version: Long, val flags: Int)
      extends CacheValue(data, version) {

   override def toString = {
      new StringBuilder().append("MemcachedValue").append("{")
         .append("data=").append(Util.printArray(data, false))
         .append(", version=").append(version)
         .append(", flags=").append(flags)
         .append("}").toString
   }

}

private class MemcachedValueExternalizer extends Externalizer {
   override def writeObject(output: ObjectOutput, obj: AnyRef) {
      val cacheValue = obj.asInstanceOf[MemcachedValue]
      output.write(cacheValue.data.length)
      output.write(cacheValue.data)
      output.writeLong(cacheValue.version)
      output.writeInt(cacheValue.flags)
   }

   override def readObject(input: ObjectInput): AnyRef = {
      val data = new Array[Byte](input.read())
      input.read(data)
      val version = input.readLong
      val flags = input.readInt
      new MemcachedValue(data, version, flags)
   }
}