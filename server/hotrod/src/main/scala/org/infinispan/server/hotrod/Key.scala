package org.infinispan.server.hotrod

import java.util.Arrays
import java.io.{ObjectOutput, ObjectInput, Externalizable}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */
// TODO: Make it an Externalizer once submodules can extend the marshalling framework
final class Key(var k: Array[Byte]) extends Externalizable {

   override def equals(obj: Any) = {
      obj match {
         case k: Key => Arrays.equals(k.k, this.k)
         case _ => false
      }
   }

   override def hashCode: Int = 41 + Arrays.hashCode(k)

   override def toString = {
      new StringBuilder().append("Key").append("{")
         .append("k=").append(k)
         .append("}").toString
   }

   override def readExternal(in: ObjectInput) {
      k = new Array[Byte](in.read())
      in.read(k)
   }

   override def writeExternal(out: ObjectOutput) {
      out.write(k.length)
      out.write(k)
   }
}