/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.server.hotrod

import java.io.{ObjectInput, ObjectOutput}
import org.infinispan.marshall.AbstractExternalizer
import scala.collection.JavaConversions._

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
   class Externalizer extends AbstractExternalizer[TopologyView] {
      override def writeObject(output: ObjectOutput, topologyView: TopologyView) {
         output.writeInt(topologyView.topologyId)
         // Write arrays instead since writing Lists causes issues
         output.writeObject(topologyView.members.toArray)
      }

      override def readObject(input: ObjectInput): TopologyView = {
         val topologyId = input.readInt
         val members = input.readObject.asInstanceOf[Array[TopologyAddress]]
         TopologyView(topologyId, members.toList)
      }

      override def getTypeClasses =
         asJavaSet(Set[java.lang.Class[_ <: TopologyView]](classOf[TopologyView]))
   }
}