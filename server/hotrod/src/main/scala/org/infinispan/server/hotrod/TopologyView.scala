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