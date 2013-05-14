package org.infinispan.server.memcached

import org.infinispan.Metadata
import org.infinispan.container.versioning.EntryVersion
import org.infinispan.Metadata.Builder
import org.infinispan.marshall.AbstractExternalizer
import java.util
import java.io.{ObjectInput, ObjectOutput}
import scala.collection.JavaConversions.setAsJavaSet

/**
 * Memcached metadata information.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
case class MemcachedMetadata(
   flags: Long,
   lifespan: Long, maxIdle: Long,
   version: EntryVersion = null, builder: Builder = null) extends Metadata

object MemcachedMetadata {

   class Externalizer extends AbstractExternalizer[MemcachedMetadata] {

      def readObject(input: ObjectInput): MemcachedMetadata = {
         val flags = input.readLong()
         val lifespan = input.readLong()
         val maxIdle = input.readLong()
         val entryVersion = input.readObject().asInstanceOf[EntryVersion]
         MemcachedMetadata(flags, lifespan, maxIdle, entryVersion)
      }

      def writeObject(output: ObjectOutput, meta: MemcachedMetadata) {
         output.writeLong(meta.flags)
         output.writeLong(meta.lifespan)
         output.writeLong(meta.maxIdle)
         output.writeObject(meta.version)
      }

      def getTypeClasses: util.Set[Class[_ <: MemcachedMetadata]] =
         setAsJavaSet(Set[java.lang.Class[_ <: MemcachedMetadata]](classOf[MemcachedMetadata]))

   }

}