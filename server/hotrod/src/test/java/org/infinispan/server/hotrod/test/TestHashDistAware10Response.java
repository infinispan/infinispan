package org.infinispan.server.hotrod.test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.infinispan.server.hotrod.ServerAddress;

public class TestHashDistAware10Response extends AbstractTestTopologyAwareResponse {

   final Map<ServerAddress, List<Integer>> hashIds;
   final int numOwners;
   final byte hashFunction;
   final int hashSpace;

   protected TestHashDistAware10Response(int topologyId, Collection<ServerAddress> members, Map<ServerAddress,
         List<Integer>> hashIds, int numOwners, byte hashFunction, int hashSpace) {
      super(topologyId, members);
      this.hashIds = hashIds;
      this.numOwners = numOwners;
      this.hashFunction = hashFunction;
      this.hashSpace = hashSpace;
   }

   @Override
   public String toString() {
      return "TestHashDistAware10Response{" +
             "numOwners=" + numOwners +
             ", hashFunction=" + hashFunction +
             ", hashSpace=" + hashSpace +
             ", topologyId=" + topologyId +
             ", members=" + members +
             '}';
   }
}
