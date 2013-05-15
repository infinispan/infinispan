package org.infinispan.server.memcached

import org.infinispan.compat.TypeConverter
import org.infinispan.context.Flag
import org.infinispan.marshall.Marshaller

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since // TODO
 */
class MemcachedTypeConverter extends TypeConverter[String, Array[Byte], String, AnyRef] {

   private var marshaller: Marshaller = _

   override def boxKey(key: String): String = key

   override def boxValue(value: Array[Byte]): AnyRef = unmarshall(value)

   override def unboxValue(target: AnyRef): Array[Byte] = marshall(target)

   override def setMarshaller(marshaller: Marshaller) {
      this.marshaller = marshaller
   }

   override def supportsInvocation(flag: Flag): Boolean =
      if (flag == Flag.OPERATION_MEMCACHED) true else false

   private def unmarshall(source: Array[Byte]): AnyRef =
      if (source != null) marshaller.objectFromByteBuffer(source) else source

   private def marshall(source: AnyRef): Array[Byte] =
      if (source != null) marshaller.objectToByteBuffer(source) else null

}
