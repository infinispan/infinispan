package org.infinispan.server.hotrod

import java.io.{ObjectOutput, ObjectInput, Externalizable}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

// TODO: Make it an Externalizer once submodules can extend the marshalling framework
final class Value(var v: Array[Byte]) extends Externalizable {

   override def toString = {
      new StringBuilder().append("Value").append("{")
         .append("v=").append(v)
         .append("}").toString
   }

   override def readExternal(in: ObjectInput) {
      v = new Array[Byte](in.read())
      in.read(v)
   }

   override def writeExternal(out: ObjectOutput) {
      out.write(v.length)
      out.write(v)
   }
}