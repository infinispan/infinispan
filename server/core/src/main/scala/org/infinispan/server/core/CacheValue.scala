package org.infinispan.server.core

import org.infinispan.commons.util.Util
import java.io.{ObjectOutput, ObjectInput}
import java.util.Arrays
import org.infinispan.commons.marshall.AbstractExternalizer
import scala.collection.JavaConversions._
import java.lang.StringBuilder

/**
 * Represents the value part of a key/value pair stored in a protocol cache.
 * With each value, a version is stored which allows for conditional operations
 * to be executed remotely in a efficient way.  For more detailed info on
 * conditional operations, check <a href="http://community.jboss.org/docs/DOC-15604">this document</a>.
 *
 * The class can be marshalled either via its externalizer or via the JVM
 * serialization.  The reason for supporting both methods is to enable
 * third-party libraries to be able to marshall/unmarshall them using standard
 * JVM serialization rules.  The Infinispan marshalling layer will always
 * chose the most performant one, aka the externalizer method.
 *
 * @author Galder Zamarreño
 * @since 4.1
 * @deprecated
 */
@deprecated
class CacheValue(val data: Array[Byte], val version: Long) extends Serializable {

   override def toString = {
      new StringBuilder().append("CacheValue").append("{")
         .append("data=").append(Util.printArray(data, false))
         .append(", version=").append(version)
         .append("}").toString
   }

   override def equals(obj: Any) = {
      obj match {
         // Apparenlty this is the way arrays should be compared for equality of contents, see:
         // http://old.nabble.com/-scala--Array-equality-td23149094.html
         case k: CacheValue => Arrays.equals(k.data, this.data) && k.version == this.version
         case _ => false
      }
   }

   override def hashCode: Int = {
      41 + Arrays.hashCode(data)
   }

}

object CacheValue {
   class Externalizer extends AbstractExternalizer[CacheValue] {
      override def writeObject(output: ObjectOutput, cacheValue: CacheValue) {
         output.writeInt(cacheValue.data.length)
         output.write(cacheValue.data)
         output.writeLong(cacheValue.version)
      }

      override def readObject(input: ObjectInput): CacheValue = {
         val data = new Array[Byte](input.readInt())
         input.readFully(data) // Must be readFully, otherwise partial arrays can be read under load!
         val version = input.readLong
         new CacheValue(data, version)
      }

      override def getTypeClasses =
         setAsJavaSet(Set[java.lang.Class[_ <: CacheValue]](classOf[CacheValue]))
   }
}
