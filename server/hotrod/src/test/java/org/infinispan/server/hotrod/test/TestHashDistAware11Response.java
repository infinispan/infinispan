package org.infinispan.server.hotrod.test;

import java.util.Map;

import org.infinispan.server.hotrod.ServerAddress;

public class TestHashDistAware11Response extends AbstractTestTopologyAwareResponse {

   final Map<ServerAddress, Integer> membersToHash;
   final int numOwners;
   final byte hashFunction;
   final int hashSpace;
   final int numVirtualNodes;

   protected TestHashDistAware11Response(int topologyId, Map<ServerAddress, Integer> membersToHash, int numOwners,
                                         byte hashFunction, int hashSpace, int numVirtualNodes) {
      super(topologyId, membersToHash.keySet());
      this.membersToHash = membersToHash;
      this.numOwners = numOwners;
      this.hashFunction = hashFunction;
      this.hashSpace = hashSpace;
      this.numVirtualNodes = numVirtualNodes;
   }

   @Override
   public String toString() {
      return "TestHashDistAware11Response{" +
             "numOwners=" + numOwners +
             ", hashFunction=" + hashFunction +
             ", hashSpace=" + hashSpace +
             ", numVirtualNodes=" + numVirtualNodes +
             ", topologyId=" + topologyId +
             ", members=" + members +
             '}';
   }
}
