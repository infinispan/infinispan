package org.infinispan.client.hotrod.impl.consistenthash;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Abstraction for the used consistent hash.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface ConsistentHash {
   
   void init(LinkedHashMap<InetSocketAddress,Integer> servers2HashCode, int numKeyOwners, int hashSpace);

   InetSocketAddress getServer(byte[] key);
}
