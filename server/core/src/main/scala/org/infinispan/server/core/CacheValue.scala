package org.infinispan.server.core

import org.infinispan.util.Util
import java.io.{Serializable, ObjectOutput, ObjectInput, Externalizable}
import org.infinispan.marshall.{Externalizer, Ids, Marshallable}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
// TODO: putting Ids.SERVER_CACHE_VALUE fails compilation in 2.8
@Marshallable(externalizer = classOf[CacheValueExternalizer], id = 55)
class CacheValue(val data: Array[Byte], val version: Long) {

   override def toString = {
      new StringBuilder().append("CacheValue").append("{")
         .append("data=").append(Util.printArray(data, false))
         .append(", version=").append(version)
         .append("}").toString
   }

}

private class CacheValueExternalizer extends Externalizer {
   override def writeObject(output: ObjectOutput, obj: AnyRef) {
      val cacheValue = obj.asInstanceOf[CacheValue]
      output.write(cacheValue.data.length)
      output.write(cacheValue.data)
      output.writeLong(cacheValue.version)
   }

   override def readObject(input: ObjectInput): AnyRef = {
      val data = new Array[Byte](input.read())
      input.read(data)
      val version = input.readLong
      new CacheValue(data, version)
   }
}
