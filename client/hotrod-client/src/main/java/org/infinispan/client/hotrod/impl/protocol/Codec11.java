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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

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
   protected Map<SocketAddress, Set<Integer>> computeNewHashes(Transport transport,
         Log localLog, int newTopologyId, int numKeyOwners,
         short hashFunctionVersion, int hashSpace, int clusterSize) {
      // New in 1.1
      int numVirtualNodes = transport.readVInt();

      if (localLog.isTraceEnabled()) {
         localLog.tracef("Topology change request: newTopologyId=%d, numKeyOwners=%d, " +
               "hashFunctionVersion=%d, hashSpaceSize=%d, clusterSize=%d, numVirtualNodes=%d",
               newTopologyId, numKeyOwners, hashFunctionVersion, hashSpace, clusterSize,
               numVirtualNodes);
      }

      Map<SocketAddress, Set<Integer>> servers2Hash =
            new LinkedHashMap<SocketAddress, Set<Integer>>();

      ConsistentHash ch = null;
      if (hashFunctionVersion != 0)
         ch = transport.getTransportFactory().getConsistentHashFactory()
               .newConsistentHash(hashFunctionVersion);
      else
         localLog.trace("Not using a consistent hash function (hash function version == 0)");

      for (int i = 0; i < clusterSize; i++) {
         String host = transport.readString();
         int port = transport.readUnsignedShort();
         // TODO: Performance improvement, since hash positions are fixed, we could maybe only calculate for those nodes that the client is not aware of?
         int baseHashCode = transport.read4ByteInt();
         int normalizedHashCode = getNormalizedHash(baseHashCode, ch);
         localLog.tracef("Server(%s:%d) read with base hash code %d, and normalized hash code %d",
               host, port, baseHashCode, normalizedHashCode);
         cacheHashCode(servers2Hash, host, port, normalizedHashCode);
         if (numVirtualNodes > 1)
            calcVirtualHashCodes(baseHashCode, numVirtualNodes, servers2Hash, host, port, ch);
      }

      return servers2Hash;
   }

   @Override
   public Log getLog() {
      return log;
   }

   private int getNormalizedHash(int baseHashCode, ConsistentHash ch) {
      if (ch != null)
         return ch.getNormalizedHash(baseHashCode);
      else
         return baseHashCode;
   }

   private void calcVirtualHashCodes(int addrHashCode, int numVirtualNodes,
            Map<SocketAddress, Set<Integer>> servers2Hash, String host, int port,
            ConsistentHash ch) {
      for (int j = 1; j < numVirtualNodes; j++) {
         int hashCode = calcVNodeHashCode(addrHashCode, j);
         cacheHashCode(servers2Hash, host, port, getNormalizedHash(hashCode, ch));
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
      getLog().tracef("Hash code is: %d", hashCode);
   }

   // IMPORTANT NOTE: Hot Rod protocol agrees to this calculation for a virtual
   // node address hash code calculation, so any changes to the implementation
   // require modification of the protocol.
   private int calcVNodeHashCode(int addrHashCode, int id) {
      int result = id;
      result = 31 * result + addrHashCode;
      return result;
   }

}
