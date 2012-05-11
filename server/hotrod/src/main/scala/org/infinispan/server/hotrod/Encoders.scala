/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.server.hotrod

import org.jboss.netty.buffer.ChannelBuffer
import org.infinispan.Cache
import org.infinispan.remoting.transport.Address
import org.infinispan.server.core.transport.ExtendedChannelBuffer._
import collection.JavaConversions._
import org.infinispan.configuration.cache.Configuration

/**
 * Version specific encoders are included here.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
object Encoders {

   /**
    * Encoder for version 1.0 of the Hot Rod protocol.
    */
   object Encoder10 extends AbstractEncoder1x

   /**
    * Encoder for version 1.1 of the Hot Rod protocol.
    */
   object Encoder11 extends AbstractEncoder1x {

      override protected def createHashDistAwareResp(lastViewId: Int,
               cfg: Configuration): AbstractHashDistAwareResponse = {
         HashDistAware11Response(lastViewId, cfg.clustering().hash().numOwners(),
               DEFAULT_HASH_FUNCTION_VERSION, Integer.MAX_VALUE,
               cfg.clustering().hash().numVirtualNodes())
      }

      override protected def writeHashTopologyHeader(
               topoResp: AbstractTopologyResponse, buf: ChannelBuffer, r: Response,
               members: Cache[Address, ServerAddress], server: HotRodServer) {
         topoResp match {
            case h: HashDistAware11Response => {
               trace("Write hash distribution change response header %s", h)
               writeCommonHashTopologyHeader(buf, h.viewId, h.numOwners,
                                             h.hashFunction, h.hashSpace, members.size)
               writeUnsignedInt(h.numVNodes, buf) // Num virtual nodes
               mapAsScalaMap(members).foreach { case (addr, serverAddr) =>
                  writeString(serverAddr.host, buf)
                  writeUnsignedShort(serverAddr.port, buf)
                  // Send the address' hash code as is
                  // With virtual nodes off, clients will have to normalize it
                  // With virtual nodes on, it's used as root to calculate
                  // hash code and then normalize it
                  buf.writeInt(if (h.hashFunction == 0) 0 else addr.hashCode())
               }
            }
            case t: TopologyAwareResponse => {
               trace("Return limited hash distribution aware header in spite of having a hash aware client %s", t)
               val serverAddresses = members.values()
               writeCommonHashTopologyHeader(buf, t.viewId, 0, 0, 0, serverAddresses.size)
               writeUnsignedInt(0, buf) // Num virtual nodes
               serverAddresses.foreach { address =>
                  writeString(address.host, buf)
                  writeUnsignedShort(address.port, buf)
                  buf.writeInt(0) // Address' hash id
               }
            }
            case _ => throw new IllegalStateException(
               "Expected version 1.1 specific response: " + topoResp)
         }
      }

   }

}
