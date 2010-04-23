package org.infinispan.server.hotrod

import java.io.{ObjectInput, ObjectOutput}
import org.infinispan.marshall.Marshallable
import org.infinispan.remoting.transport.Address

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since // TODO
 */
@Marshallable(externalizer = classOf[TopologyAddress.Externalizer], id = 58)
case class TopologyAddress(val host: String, val port: Int, val hostHashCode: Int, val clusterAddress: Address)

object TopologyAddress {
   class Externalizer extends org.infinispan.marshall.Externalizer {
      override def writeObject(output: ObjectOutput, obj: AnyRef) {
         val topologyAddress = obj.asInstanceOf[TopologyAddress]
         output.writeObject(topologyAddress.host)
         output.writeInt(topologyAddress.port)
         output.writeInt(topologyAddress.hostHashCode)
         output.writeObject(topologyAddress.clusterAddress)
      }

      override def readObject(input: ObjectInput): AnyRef = {
         val host = input.readObject.asInstanceOf[String]
         val port = input.readInt
         val hostHashCode = input.readInt
         val clusterAddress = input.readObject.asInstanceOf[Address]
         TopologyAddress(host, port, hostHashCode, clusterAddress)
      }
   }
}