package org.infinispan.server.hotrod

import org.infinispan.marshall.Marshallable
import java.io.{ObjectInput, ObjectOutput}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since // TODO
 */
@Marshallable(externalizer = classOf[TopologyView.Externalizer], id = 59)
case class TopologyView(val topologyId: Int, val members: List[TopologyAddress])
// TODO: TopologyView could maintain a Map[Address, TopologyAddress] rather than pushing Address into each TopologyAddress.
// TODO: That would make crash detection more efficient at the expense of some extra space.
// TODO: In fact, it might increase more concurrency and make replication more efficient if topology cache stored stuff
// TODO: in [Address, TopologyAddress] and either keep the topology id as an entry in that same cache or in a separate one.
// TODO: The downside here is that you'd need to make multiple cache calls atomic via txs or similar.

object TopologyView {
   class Externalizer extends org.infinispan.marshall.Externalizer {
      override def writeObject(output: ObjectOutput, obj: AnyRef) {
         val topologyView = obj.asInstanceOf[TopologyView]
         output.writeInt(topologyView.topologyId)
         output.writeObject(topologyView.members.toArray) // Write arrays instead since writing Lists causes issues
      }

      override def readObject(input: ObjectInput): AnyRef = {
         val topologyId = input.readInt
         val members = input.readObject.asInstanceOf[Array[TopologyAddress]]
         TopologyView(topologyId, members.toList)
      }
   }
}