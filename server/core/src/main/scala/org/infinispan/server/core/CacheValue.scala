package org.infinispan.server.core

import org.infinispan.util.Util
import java.io.{Serializable, ObjectOutput, ObjectInput, Externalizable}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */
// TODO: Make it a hardcoded Externalizer
class CacheValue(val data: Array[Byte], val version: Long) extends Serializable {

   override def toString = {
      new StringBuilder().append("CacheValue").append("{")
         .append("data=").append(Util.printArray(data, false))
         .append(", version=").append(version)
         .append("}").toString
   }

//   override def readExternal(in: ObjectInput) {
//      data = new Array[Byte](in.read())
//      in.read(data)
//      version = in.readLong
//   }
//
//   override def writeExternal(out: ObjectOutput) {
//      out.write(data.length)
//      out.write(data)
//      out.writeLong(version)
//   }
}