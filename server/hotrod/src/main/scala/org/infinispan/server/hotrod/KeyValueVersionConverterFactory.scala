package org.infinispan.server.hotrod

import java.io.{ObjectInput, ObjectOutput}

import org.infinispan.commons.io.UnsignedNumeric
import org.infinispan.commons.marshall.AbstractExternalizer
import org.infinispan.container.versioning.NumericVersion
import org.infinispan.metadata.Metadata
import org.infinispan.notifications.cachelistener.filter.{CacheEventConverter, CacheEventConverterFactory, EventType}
import org.infinispan.server.hotrod.KeyValueVersionConverterFactory.KeyValueVersionConverter

import scala.collection.JavaConversions

class KeyValueVersionConverterFactory extends CacheEventConverterFactory {
   override def getConverter[K, V, C](params: Array[AnyRef]): CacheEventConverter[K, V, C] =
      KeyValueVersionConverter.Singleton.asInstanceOf[CacheEventConverter[K, V, C]] // ugly but it works :|
}

object KeyValueVersionConverterFactory {
   class KeyValueVersionConverter extends CacheEventConverter[Bytes, Bytes, Bytes] {
      override def convert(key: Bytes, oldValue: Bytes, oldMetadata: Metadata, newValue: Bytes, newMetadata: Metadata, eventType: EventType): Bytes = {
         val capacity = UnsignedNumeric.sizeUnsignedInt(key.length) + key.length +
            (if (newValue != null) UnsignedNumeric.sizeUnsignedInt(newValue.length) + newValue.length + 8 else 0)

         val out = Array.ofDim[Byte](capacity)
         var offset = UnsignedNumeric.writeUnsignedInt(out, 0, key.length)
         offset = putBytes(key, offset, out)
         if (newValue != null) {
            offset = UnsignedNumeric.writeUnsignedInt(out, offset, newValue.length)
            offset = putBytes(newValue, offset, out)
            putLong(newMetadata.version().asInstanceOf[NumericVersion].getVersion, offset, out)
         }
         out
      }
   }

   private def putBytes(bytes: Array[Byte], offset: Int, out: Array[Byte]): Int = {
      var localOffset = offset
      bytes.foreach(b => {
         out.update(localOffset, b)
         localOffset += 1
      })
      localOffset
   }

   private def putLong(l: Long, offset: Int, out: Array[Byte]): Int = {
      out.update(offset, (l >> 56).toByte)
      out.update(offset + 1, (l >> 48).toByte)
      out.update(offset + 2, (l >> 40).toByte)
      out.update(offset + 3, (l >> 32).toByte)
      out.update(offset + 4, (l >> 24).toByte)
      out.update(offset + 5, (l >> 16).toByte)
      out.update(offset + 6, (l >> 8).toByte)
      out.update(offset + 7, l.toByte)
      offset + 8
   }

   object KeyValueVersionConverter {
      val Singleton = new KeyValueVersionConverter

      class Externalizer extends AbstractExternalizer[KeyValueVersionConverter] {
         override def getTypeClasses = JavaConversions.setAsJavaSet(Set[java.lang.Class[_ <: KeyValueVersionConverter]](classOf[KeyValueVersionConverter]))
         override def readObject(input: ObjectInput): KeyValueVersionConverter = Singleton
         override def writeObject(output: ObjectOutput, `object`: KeyValueVersionConverter): Unit = {}
      }
   }
}
