package org.infinispan.server.memcached

import org.infinispan.compat.TypeConverter
import org.infinispan.context.Flag
import org.infinispan.commons.marshall.{JavaSerializationMarshaller, Marshaller}

/**
 * Type converter that transforms Memcached data so that it can be accessible
 * via other endpoints.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
class MemcachedTypeConverter extends TypeConverter[String, AnyRef, String, AnyRef] {

   // Default marshaller needed in case no custom marshaller is set
   // (e.g. not using Spy Memcached client). This is because in compatibility
   // mode, data is stored unmarshalled, so when returning data to Memcached
   // clients, it needs to be marshalled to fulfill the Memcached protocol.
   //
   // A generic marshaller using Java Serialization is used by default, since
   // that's the safest bet to support alternative Java Memcached clients
   private var marshaller: Marshaller = new JavaSerializationMarshaller

   override def boxKey(key: String): String = key

   override def boxValue(value: AnyRef): AnyRef = unmarshall(value)

   override def unboxKey(key: String): String = key

   override def unboxValue(target: AnyRef): Array[Byte] = marshall(target)

   override def setMarshaller(marshaller: Marshaller) {
      this.marshaller = marshaller
   }

   override def supportsInvocation(flag: Flag): Boolean =
      if (flag == Flag.OPERATION_MEMCACHED) true else false

   private def unmarshall(source: AnyRef): AnyRef = source match {
      case bytes: Array[Byte] => marshaller.objectFromByteBuffer(bytes)
      case _ => source
   }

   private def marshall(source: AnyRef): Array[Byte] =
      if (source != null) marshaller.objectToByteBuffer(source) else null

}
