package org.infinispan.server.core

import org.infinispan.util.Util
import java.io.{Serializable, ObjectOutput, ObjectInput, Externalizable}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */
// TODO: Make it a hardcoded Externalizer
class Value(val v: Array[Byte], val version: Long) extends Serializable {

   override def toString = {
      new StringBuilder().append("Value").append("{")
         .append("v=").append(Util.printArray(v, false))
         .append(", version=").append(version)
         .append("}").toString
   }

//   override def readExternal(in: ObjectInput) {
//      v = new Array[Byte](in.read())
//      in.read(v)
//      version = in.readLong
//   }
//
//   override def writeExternal(out: ObjectOutput) {
//      out.write(v.length)
//      out.write(v)
//      out.writeLong(version)
//   }
}