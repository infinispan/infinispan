/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Hot Rod encoder/decoder for version 1.1 of the protocol.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class Codec11 extends Codec10 {

   private static final Log log = LogFactory.getLog(Codec11.class, Log.class);

   @Override
   public HeaderParams writeHeader(Transport transport, HeaderParams params) {
      return writeHeader(transport, params, HotRodConstants.VERSION_11);
   }

   @Override
   protected void readNewTopologyAndHash(Transport transport, AtomicInteger topologyId) {
      int newTopologyId = transport.readVInt();
      topologyId.set(newTopologyId);
      int numKeyOwners = transport.readUnsignedShort();
      short hashFctVersion = transport.readByte();
      if (hashFctVersion == 0) {
         log.trace("Not using a consistent hash function (hash function version == 0).");
         // Client can receive hash topology updates but cache is not configured with distribution
         transport.readVInt(); // Ignore hash space
         int clusterSize = transport.readVInt();
         transport.readVInt(); // Ignore virtual nodes
         Set<SocketAddress> addresses = new HashSet<SocketAddress>();
         for (int i = 0; i < clusterSize; i++) {
            String host = transport.readString();
            int port = transport.readUnsignedShort();
            log.tracef("Server read: %s:%d", host, port);
            addresses.add(new InetSocketAddress(host, port));
         }

         if (log.isInfoEnabled())
            log.newTopology(addresses);

         transport.getTransportFactory().updateServers(addresses);
      } else {
         ConsistentHash ch = transport.getTransportFactory()
               .getConsistentHashFactory().newConsistentHash(hashFctVersion);
         int hashSpace = transport.readVInt();
         int clusterSize = transport.readVInt();
         // New in 1.1
         int numVirtualNodes = transport.readVInt();

         log.tracef("Topology change request: newTopologyId=%d, numKeyOwners=%d, " +
               "hashFunctionVersion=%d, hashSpaceSize=%d, clusterSize=%d, numVirtualNodes=%d",
               newTopologyId, numKeyOwners, hashFctVersion, hashSpace, clusterSize,
               numVirtualNodes);

         Map<SocketAddress, Set<Integer>> servers2Hash =
               new LinkedHashMap<SocketAddress, Set<Integer>>();

         for (int i = 0; i < clusterSize; i++) {
            String host = transport.readString();
            int port = transport.readUnsignedShort();
            log.tracef("Server read: %s:%d", host, port);
            // TODO: Performance improvement, since hash positions are fixed, we could maybe only calculate for those nodes that the client is not aware of?
            int hashCode = calcNodeHashCode(host, port, ch);
            cacheHashCode(servers2Hash, host, port, hashCode);
            if (numVirtualNodes > 1)
               calcVirtualHashCodes(ch, numVirtualNodes, servers2Hash, host, port);
         }

         if (log.isInfoEnabled()) {
            log.newTopology(servers2Hash.keySet());
         }
         transport.getTransportFactory().updateServers(servers2Hash.keySet());
         transport.getTransportFactory().updateHashFunction(
               servers2Hash, numKeyOwners, hashFctVersion, hashSpace);
      }
   }

   private void calcVirtualHashCodes(ConsistentHash ch, int numVirtualNodes,
            Map<SocketAddress, Set<Integer>> servers2Hash, String host, int port) {
      for (int j = 1; j < numVirtualNodes; j++) {
         int hashCode = calcVNodeHashCode(host, port, ch, j);
         cacheHashCode(servers2Hash, host, port, hashCode);
      }
   }

   private void cacheHashCode(Map<SocketAddress, Set<Integer>> servers2Hash,
            String host, int port, int hashCode) {
      InetSocketAddress address = new InetSocketAddress(host, port);
      Set<Integer> hashes = servers2Hash.get(address);
      if (hashes == null) {
         hashes = new HashSet<Integer>();
         servers2Hash.put(address, hashes);
      }
      hashes.add(hashCode);
      log.tracef("Hash code is: %d", hashCode);
   }

   // IMPORTANT NOTE: Hot Rod protocol agrees to this calculation for a node
   // address hash code calculation, so any changes to the implementation
   // require modification of the protocol.
   private int calcNodeHashCode(String host, int port, ConsistentHash ch) {
      byte[] addressBytes = String.format("%s:%d", host, port)
            .getBytes(HotRodConstants.HOTROD_STRING_CHARSET);
      return ch.getNormalizedHash(Arrays.hashCode(addressBytes));
   }

   // IMPORTANT NOTE: Hot Rod protocol agrees to this calculation for a virtual
   // node address hash code calculation, so any changes to the implementation
   // require modification of the protocol.
   private int calcVNodeHashCode(String host, int port, ConsistentHash ch, int id) {
      int addrHashCode = calcNodeHashCode(host, port, ch);
      int result = id;
      result = 31 * result + addrHashCode;
      return result;
   }

}
