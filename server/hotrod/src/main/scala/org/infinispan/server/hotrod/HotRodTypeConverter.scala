package org.infinispan.server.hotrod

import org.infinispan.compat.TypeConverter
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller
import org.infinispan.context.Flag
import org.infinispan.commons.marshall.Marshaller

/**
 * Hot Rod type converter for compatibility mode.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
class HotRodTypeConverter extends TypeConverter[AnyRef, AnyRef, AnyRef, AnyRef] {

   // Default marshaller is the one used by the Hot Rod client,
   // but can be configured for compatibility use cases
   private var marshaller: Marshaller = new GenericJBossMarshaller

   override def boxKey(key: AnyRef): AnyRef = unmarshall(key)

   override def boxValue(value: AnyRef): AnyRef = unmarshall(value)

   override def unboxKey(target: AnyRef): Array[Byte] = marshall(target)

   override def unboxValue(target: AnyRef): Array[Byte] = marshall(target)

   override def supportsInvocation(flag: Flag): Boolean =
      if (flag == Flag.OPERATION_HOTROD) true else false

   override def setMarshaller(marshaller: Marshaller) {
      this.marshaller = marshaller
   }

   private def unmarshall(source: AnyRef): AnyRef = {
      source match {
         case bytes: Array[Byte] =>
            marshaller.objectFromByteBuffer(bytes)
         case _ => source
      }
   }

   private def marshall(source: AnyRef): Array[Byte] =
      if (source != null) marshaller.objectToByteBuffer(source) else null

}
