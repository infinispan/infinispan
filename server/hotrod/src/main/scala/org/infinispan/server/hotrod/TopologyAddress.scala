package org.infinispan.server.hotrod

import java.io.{ObjectInput, ObjectOutput}
import org.infinispan.marshall.Marshallable
import org.infinispan.remoting.transport.Address

/**
 * A Hot Rod topology address represents a Hot Rod endpoint that belongs to a Hot Rod cluster. It contains host/port
 * information where the Hot Rod endpoint is listening. To be able to detect crashed members in the cluster and update
 * the Hot Rod topology accordingly, it also contains the corresponding cluster address. Finally, since each cache
 * could potentially be configured with a different hash algorithm, a topology address also contains per cache hash id.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
@Marshallable(externalizer = classOf[TopologyAddress.Externalizer], id = 58)
case class TopologyAddress(val host: String, val port: Int, val hashIds: Map[String, Int], val clusterAddress: Address)

object TopologyAddress {
   class Externalizer extends org.infinispan.marshall.Externalizer[TopologyAddress] {
      override def writeObject(output: ObjectOutput, topologyAddress: TopologyAddress) {
         output.writeObject(topologyAddress.host)
         output.writeInt(topologyAddress.port)
         output.writeObject(topologyAddress.hashIds)
         output.writeObject(topologyAddress.clusterAddress)
      }

      override def readObject(input: ObjectInput): TopologyAddress = {
         val host = input.readObject.asInstanceOf[String]
         val port = input.readInt
         val hashIds = input.readObject.asInstanceOf[Map[String, Int]]
         val clusterAddress = input.readObject.asInstanceOf[Address]
         TopologyAddress(host, port, hashIds, clusterAddress)
      }
   }
}