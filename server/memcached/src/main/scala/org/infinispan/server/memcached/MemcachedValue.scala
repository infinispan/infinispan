package org.infinispan.server.memcached

import org.infinispan.server.core.CacheValue
import org.infinispan.util.Util
import java.io.{ObjectOutput, ObjectInput}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */
// TODO: Make it a hardcoded Externalizer
class MemcachedValue(override val data: Array[Byte], override val version: Long, val flags: Int)
      extends CacheValue(data, version) {

   override def toString = {
      new StringBuilder().append("MemcachedValue").append("{")
         .append("data=").append(Util.printArray(data, false))
         .append(", version=").append(version)
         .append(", flags=").append(flags)
         .append("}").toString
   }

//   override def readExternal(in: ObjectInput) {
////      data = new Array[Byte](in.read())
////      in.read(data)
////      version = in.readLong
//      super.readExternal(in)
//      flags = in.readInt
//   }
//
//   override def writeExternal(out: ObjectOutput) {
//      super.writeExternal(out)
////      out.write(data.length)
////      out.write(data)
////      out.writeLong(version)
//      out.writeInt(flags)
//   }

}