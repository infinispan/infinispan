package org.infinispan.rest

import org.infinispan.container.versioning.EntryVersion
import java.util.concurrent.TimeUnit.{MILLISECONDS => MILLIS}
import java.util.concurrent.TimeUnit
import org.infinispan.metadata.{EmbeddedMetadata, Metadata}
import Metadata.Builder
import org.infinispan.commons.marshall.AbstractExternalizer
import java.util
import java.io.{ObjectInput, ObjectOutput}
import scala.collection.JavaConversions.setAsJavaSet
import org.jboss.marshalling.util.IdentityIntMap

/**
 * Metadata for MIME data stored in REST servers.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
class MimeMetadata(val contentType: String) extends Metadata {

   def lifespan(): Long = -1

   def maxIdle(): Long = -1

   def version(): EntryVersion = null

   def builder(): Builder = new MimeMetadataBuilder().contentType(contentType)

   override def equals(obj: Any): Boolean = {
      obj match {
         case that: MimeMetadata =>
            (that.canEqual(this)) && contentType == that.contentType
         case _ => false
      }
   }

   def canEqual(other: Any): Boolean = other.isInstanceOf[MimeMetadata]

   override def hashCode(): Int = 41 + contentType.hashCode

   override def toString: String = s"MimeMetadata(contentType=$contentType)"

}

private class MimeExpirableMetadata(override val contentType: String,
        lifespanTime: Long, lifespanUnit: TimeUnit,
        maxIdleTime: Long, maxIdleUnit: TimeUnit) extends MimeMetadata(contentType) {

   override final val lifespan = lifespanUnit.toMillis(lifespanTime)

   override final val maxIdle = maxIdleUnit.toMillis(maxIdleTime)

   override def equals(obj: Any): Boolean = {
      obj match {
         case that: MimeExpirableMetadata =>
            (that.canEqual(this)) &&
                    contentType == that.contentType &&
                    lifespan == that.lifespan &&
                    maxIdle == that.maxIdle
         case _ => false
      }
   }

   override def canEqual(other: Any): Boolean = other.isInstanceOf[MimeExpirableMetadata]

   override def hashCode(): Int =
      41 * (41 * (41 + contentType.hashCode) + lifespan.toInt) + maxIdle.toInt

   override def toString: String =
      s"MimeExpirableMetadata(contentType=$contentType, lifespan=$lifespan, maxIdle=$maxIdle)"

}

private class MimeMetadataBuilder extends EmbeddedMetadata.Builder {

   private var contentType: String = _

   def contentType(contentType: String): MimeMetadataBuilder = {
      this.contentType = contentType
      this
   }

   override def build(): Metadata =
      MimeMetadata(contentType, lifespan, lifespanUnit, maxIdle, maxIdleUnit)

}

object MimeMetadata {

   def apply(contentType: String,
           lifespan: Long, lifespanUnit: TimeUnit,
           maxIdle: Long, maxIdleUnit: TimeUnit): MimeMetadata = {
      if (lifespan < 0 && maxIdle < 0)
         new MimeMetadata(contentType)
      else
         new MimeExpirableMetadata(contentType, lifespan, lifespanUnit, maxIdle, maxIdleUnit)
   }

   private def apply(contentType: String): MimeMetadata = new MimeMetadata(contentType)

   class Externalizer extends AbstractExternalizer[MimeMetadata] {

      final val Immortal = 0
      final val Expirable = 1

      final val numbers = new IdentityIntMap[Class[_]](2)

      numbers.put(classOf[MimeMetadata], Immortal)
      numbers.put(classOf[MimeExpirableMetadata], Expirable)

      def readObject(input: ObjectInput): MimeMetadata = {
         val contentType = input.readUTF()
         val number = input.readUnsignedByte()
         number match {
            case Immortal => MimeMetadata(contentType)
            case Expirable =>
               val lifespan = input.readLong()
               val maxIdle = input.readLong()
               MimeMetadata(contentType, lifespan, MILLIS, maxIdle, MILLIS)
         }
      }

      def writeObject(output: ObjectOutput, meta: MimeMetadata) {
         output.writeUTF(meta.contentType)
         val number = numbers.get(meta.getClass, -1)
         output.write(number)
         if (number == Expirable) {
            output.writeLong(meta.lifespan())
            output.writeLong(meta.maxIdle())
         }
      }

      def getTypeClasses: util.Set[Class[_ <: MimeMetadata]] =
         setAsJavaSet(Set[java.lang.Class[_ <: MimeMetadata]](
            classOf[MimeMetadata], classOf[MimeExpirableMetadata]))

   }

}
