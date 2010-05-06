package org.infinispan.client.hotrod.impl.consistenthash;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class ConsitentHashV1 implements ConsistentHash {

   private static Log log = LogFactory.getLog(ConsitentHashV1.class);
   
   private volatile Map<InetSocketAddress, Integer> servers2HashCode;

   private volatile int numKeyOwners;

   private volatile int hashSpace;

   private Random random = new Random();
   List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();

   @Override
   public void init(LinkedHashMap<InetSocketAddress,Integer> servers2HashCode, int numKeyOwners, int hashSpace) {
      this.servers2HashCode = servers2HashCode;
      this.numKeyOwners = numKeyOwners;
      this.hashSpace = hashSpace;
      addresses.addAll(servers2HashCode.keySet());
   }

   @Override
   public InetSocketAddress getServer(byte[] key) {
      InetSocketAddress addr = addresses.get(random.nextInt(addresses.size()));
      if (log.isTraceEnabled()) {
         log.trace("Randomly returning an address: " + addr);
      }
      return addr;
   }
}
