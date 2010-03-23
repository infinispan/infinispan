package org.infinispan.server.memcached

import org.infinispan.server.core.Value
import org.infinispan.util.Util
import java.io.{ObjectOutput, ObjectInput}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */
// TODO: Make it a hardcoded Externalizer
class MemcachedValue(override val v: Array[Byte], override val version: Long, val flags: Int)
      extends Value(v, version) {

   override def toString = {
      new StringBuilder().append("MemcachedValue").append("{")
         .append("v=").append(Util.printArray(v, false))
         .append(", version=").append(version)
         .append(", flags=").append(flags)
         .append("}").toString
   }

//   override def readExternal(in: ObjectInput) {
////      v = new Array[Byte](in.read())
////      in.read(v)
////      version = in.readLong
//      super.readExternal(in)
//      flags = in.readInt
//   }
//
//   override def writeExternal(out: ObjectOutput) {
//      super.writeExternal(out)
////      out.write(v.length)
////      out.write(v)
////      out.writeLong(version)
//      out.writeInt(flags)
//   }

}