package org.infinispan.client.hotrod.impl.consistenthash;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.infinispan.util.hash.MurmurHash2.hash;

/**
 * Version one consistent hash function based on {@link org.infinispan.util.hash.MurmurHash2};
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class ConsistentHashV1 implements ConsistentHash {

   private static final Log log = LogFactory.getLog(ConsistentHashV1.class);
   
   private final SortedMap<Integer, InetSocketAddress> positions = new TreeMap<Integer, InetSocketAddress>();

   private volatile int hashSpace;

   @Override
   public void init(LinkedHashMap<InetSocketAddress,Integer> servers2HashCode, int numKeyOwners, int hashSpace) {
      for (InetSocketAddress addr :servers2HashCode.keySet()) {
         positions.put(servers2HashCode.get(addr), addr);
      }
      if (log.isTraceEnabled())
         log.trace("Positions are: " + positions);
      this.hashSpace = hashSpace;
   }

   @Override
   public InetSocketAddress getServer(byte[] key) {
      int keyHashCode = hash(key);
      if (keyHashCode == Integer.MIN_VALUE) keyHashCode += 1;
      int hash = Math.abs(keyHashCode);

      SortedMap<Integer, InetSocketAddress> candidates = positions.tailMap(hash % hashSpace);
      if (log.isTraceEnabled()) {
         log.trace("Found possible candidates: " + candidates);
      }
      if (candidates.isEmpty()) {
         InetSocketAddress socketAddress = positions.get(positions.firstKey());
         if (log.isTraceEnabled()) {
            log.trace("Over the wheel, returning first member: " + socketAddress);
         }
         return socketAddress;
      } else {
         InetSocketAddress socketAddress = candidates.get(candidates.firstKey());
         log.trace("Found candidate: " + socketAddress);
         return socketAddress;
      }
   }
}
