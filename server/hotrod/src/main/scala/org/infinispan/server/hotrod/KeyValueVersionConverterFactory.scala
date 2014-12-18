package org.infinispan.server.hotrod

import java.io.{ObjectOutput, ObjectInput}
import java.nio.ByteBuffer

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

         val out = ByteBuffer.allocate(capacity)
         UnsignedNumeric.writeUnsignedInt(out, key.length)
         out.put(key)
         if (newValue != null) {
            UnsignedNumeric.writeUnsignedInt(out, newValue.length)
            out.put(newValue)
            out.putLong(newMetadata.version().asInstanceOf[NumericVersion].getVersion)
         }

         out.array()
      }
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
