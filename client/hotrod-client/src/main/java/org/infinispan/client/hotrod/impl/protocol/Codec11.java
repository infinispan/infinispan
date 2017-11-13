package org.infinispan.client.hotrod.impl.protocol;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;

/**
 * A Hot Rod encoder/decoder for version 1.1 of the protocol.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class Codec11 extends Codec10 {

   private static final Log log = LogFactory.getLog(Codec11.class, Log.class);

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_11);
   }

   @Override
   protected Map<SocketAddress, Set<Integer>> computeNewHashes(ByteBuf buf, ChannelFactory channelFactory,
                                                               Log localLog, int newTopologyId, int numKeyOwners,
                                                               short hashFunctionVersion, int hashSpace, int clusterSize) {
      // New in 1.1
      int numVirtualNodes = ByteBufUtil.readVInt(buf);

      if (trace) {
         localLog.tracef("Topology change request: newTopologyId=%d, numKeyOwners=%d, " +
               "hashFunctionVersion=%d, hashSpaceSize=%d, clusterSize=%d, numVirtualNodes=%d",
               newTopologyId, numKeyOwners, hashFunctionVersion, hashSpace, clusterSize,
               numVirtualNodes);
      }

      Map<SocketAddress, Set<Integer>> servers2Hash =
            new LinkedHashMap<SocketAddress, Set<Integer>>();

      ConsistentHash ch = null;
      if (hashFunctionVersion == 0) {
         localLog.trace("Not using a consistent hash function (version 0)");
      } else if (hashFunctionVersion == 1) {
         localLog.trace("Ignoring obsolete consistent hash function (version 1)");
      } else {
         ch = channelFactory.getConsistentHashFactory().newConsistentHash(hashFunctionVersion);
      }

      for (int i = 0; i < clusterSize; i++) {
         String host = ByteBufUtil.readString(buf);
         int port = buf.readUnsignedShort();
         // TODO: Performance improvement, since hash positions are fixed, we could maybe only calculate for those nodes that the client is not aware of?
         int baseHashCode = buf.readIntLE();
         int normalizedHashCode = getNormalizedHash(baseHashCode, ch);
         if (trace) localLog.tracef("Server(%s:%d) read with base hash code %d, and normalized hash code %d",
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
      SocketAddress address = InetSocketAddress.createUnresolved(host, port);
      Set<Integer> hashes = servers2Hash.get(address);
      if (hashes == null) {
         hashes = new HashSet<Integer>();
         servers2Hash.put(address, hashes);
      }
      hashes.add(hashCode);
      if (trace) getLog().tracef("Hash code is: %d", hashCode);
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
