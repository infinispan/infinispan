package org.infinispan.server.hotrod

import java.io.{ObjectInput, ObjectOutput}
import org.infinispan.marshall.Marshallable

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since // TODO
 */
@Marshallable(externalizer = classOf[TopologyAddress.Externalizer], id = 58)
case class TopologyAddress(val host: String, val port: Int, val hostHashCode: Int)

object TopologyAddress {
   class Externalizer extends org.infinispan.marshall.Externalizer {
      override def writeObject(output: ObjectOutput, obj: AnyRef) {
         val topologyAddress = obj.asInstanceOf[TopologyAddress]
         output.writeObject(topologyAddress.host)
         output.writeInt(topologyAddress.port)
         output.writeInt(topologyAddress.hostHashCode)
      }

      override def readObject(input: ObjectInput): AnyRef = {
         val host = input.readObject.asInstanceOf[String]
         val port = input.readInt
         val hostHashCode = input.readInt
         TopologyAddress(host, port, hostHashCode)
      }
   }
}