package org.infinispan.server.hotrod.test;

import java.util.Collection;

import org.infinispan.server.hotrod.ServerAddress;

public class TestTopologyAwareResponse extends AbstractTestTopologyAwareResponse {
   protected TestTopologyAwareResponse(int topologyId, Collection<ServerAddress> members) {
      super(topologyId, members);
   }

   @Override
   public String toString() {
      return "TestTopologyAwareResponse{" +
             "topologyId=" + topologyId +
             ", members=" + members +
             '}';
   }
}
