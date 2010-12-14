package org.infinispan.server.hotrod

import org.infinispan.marshall.Marshalls
import java.io.{ObjectInput, ObjectOutput}

/**
 * A Hot Rod server topology view.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
case class TopologyView(val topologyId: Int, val members: List[TopologyAddress])
// TODO: TopologyView could maintain a Map[Address, TopologyAddress] rather than pushing Address into each TopologyAddress.
// TODO: That would make crash detection more efficient at the expense of some extra space.
// TODO: In fact, it might increase more concurrency and make replication more efficient if topology cache stored stuff
// TODO: in [Address, TopologyAddress] and either keep the topology id as an entry in that same cache or in a separate one.
// TODO: The downside here is that you'd need to make multiple cache calls atomic via txs or similar.

object TopologyView {
   @Marshalls(typeClasses = Array(classOf[TopologyView]))
   class Externalizer extends org.infinispan.marshall.Externalizer[TopologyView] {
      override def writeObject(output: ObjectOutput, topologyView: TopologyView) {
         output.writeInt(topologyView.topologyId)
         output.writeObject(topologyView.members.toArray) // Write arrays instead since writing Lists causes issues
      }

      override def readObject(input: ObjectInput): TopologyView = {
         val topologyId = input.readInt
         val members = input.readObject.asInstanceOf[Array[TopologyAddress]]
         TopologyView(topologyId, members.toList)
      }
   }
}