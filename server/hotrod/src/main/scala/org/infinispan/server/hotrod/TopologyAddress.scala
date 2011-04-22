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
import org.infinispan.remoting.transport.Address
import org.infinispan.marshall.AbstractExternalizer
import scala.collection.JavaConversions._

/**
 * A Hot Rod topology address represents a Hot Rod endpoint that belongs to a Hot Rod cluster. It contains host/port
 * information where the Hot Rod endpoint is listening. To be able to detect crashed members in the cluster and update
 * the Hot Rod topology accordingly, it also contains the corresponding cluster address. Finally, since each cache
 * could potentially be configured with a different hash algorithm, a topology address also contains per cache hash id.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
case class TopologyAddress(val host: String, val port: Int, val hashIds: Map[String, Int], val clusterAddress: Address)

object TopologyAddress {
   class Externalizer extends AbstractExternalizer[TopologyAddress] {
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

      override def getTypeClasses =
         asJavaSet(Set[java.lang.Class[_ <: TopologyAddress]](classOf[TopologyAddress]))
   }
}