package org.infinispan.server.memcached

import org.infinispan.container.versioning.EntryVersion
import org.infinispan.metadata.{EmbeddedMetadata, Metadata}
import Metadata.Builder
import org.infinispan.commons.marshall.AbstractExternalizer
import java.util
import java.io.{ObjectInput, ObjectOutput}
import scala.collection.JavaConversions.setAsJavaSet
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.{MILLISECONDS => MILLIS}
import org.jboss.marshalling.util.IdentityIntMap

/**
 * Memcached metadata information.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
class MemcachedMetadata(val flags: Long, val version: EntryVersion) extends Metadata {

   def lifespan(): Long = -1

   def maxIdle(): Long = -1

   def builder(): Builder = new MemcachedMetadataBuilder().flags(flags).version(version)

   override def equals(obj: Any): Boolean = {
      obj match {
         case that: MemcachedMetadata =>
            that.canEqual(this) &&
                    flags == that.flags &&
                    version == that.version
         case _ => false
      }
   }

   def canEqual(other: Any): Boolean = other.isInstanceOf[MemcachedMetadata]

   override def hashCode(): Int = 41 * (41 + flags.hashCode) + version.hashCode()

   override def toString: String = s"MemcachedMetadata(flags=$flags, version=$version)"

}

private class MemcachedExpirableMetadata(
        override val flags: Long, override val version: EntryVersion,
        lifespanTime: Long, lifespanUnit: TimeUnit) extends MemcachedMetadata(flags, version) {

   override final val lifespan = lifespanUnit.toMillis(lifespanTime)

   override def builder(): Builder = new MemcachedMetadataBuilder()
         .flags(flags).version(version).lifespan(lifespan)

   override def equals(obj: Any): Boolean = {
      obj match {
         case that: MemcachedExpirableMetadata =>
            that.canEqual(this) &&
                    flags == that.flags &&
                    version == that.version &&
                    lifespan == that.lifespan
         case _ => false
      }
   }

   override def canEqual(other: Any): Boolean = other.isInstanceOf[MemcachedExpirableMetadata]

   override def hashCode(): Int =
      41 * (41 * (41 + flags.hashCode) + version.hashCode) + lifespan.toInt

   override def toString: String =
      s"MemcachedExpirableMetadata(flags=$flags, version=$version, lifespan=$lifespan)"

}

class MemcachedMetadataBuilder extends EmbeddedMetadata.Builder {

   private var flags: Long = _

   def flags(flags: Long): MemcachedMetadataBuilder = {
      this.flags = flags
      this
   }

   override def build(): Metadata = {
      if (hasLifespan)
         new MemcachedExpirableMetadata(flags, version, lifespan, lifespanUnit)
      else
         new MemcachedMetadata(flags, version)
   }
}

object MemcachedMetadata {

   class Externalizer extends AbstractExternalizer[MemcachedMetadata] {

      final val Immortal = 0
      final val Expirable = 1

      final val numbers = new IdentityIntMap[Class[_]](2)

      numbers.put(classOf[MemcachedMetadata], Immortal)
      numbers.put(classOf[MemcachedExpirableMetadata], Expirable)

      def readObject(input: ObjectInput): MemcachedMetadata = {
         val flags = input.readLong()
         val version = input.readObject().asInstanceOf[EntryVersion]
         val number = input.readUnsignedByte()
         number match {
            case Immortal => new MemcachedMetadata(flags, version)
            case Expirable =>
               val lifespan = input.readLong()
               new MemcachedExpirableMetadata(flags, version, lifespan, MILLIS)
         }
      }

      def writeObject(output: ObjectOutput, meta: MemcachedMetadata) {
         output.writeLong(meta.flags)
         output.writeObject(meta.version)
         val number = numbers.get(meta.getClass, -1)
         output.write(number)
         if (number == Expirable) {
            output.writeLong(meta.lifespan())
         }
      }

      def getTypeClasses: util.Set[Class[_ <: MemcachedMetadata]] =
         setAsJavaSet(Set[java.lang.Class[_ <: MemcachedMetadata]](
            classOf[MemcachedMetadata], classOf[MemcachedExpirableMetadata]))

   }

}
