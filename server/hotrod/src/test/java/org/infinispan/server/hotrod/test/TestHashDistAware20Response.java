package org.infinispan.server.hotrod.test;

import java.util.Collection;
import java.util.List;

import org.infinispan.server.hotrod.ServerAddress;

public class TestHashDistAware20Response extends AbstractTestTopologyAwareResponse {
   final List<Iterable<ServerAddress>> segments;
   final byte hashFunction;

   protected TestHashDistAware20Response(int topologyId, Collection<ServerAddress> members,
                                         List<Iterable<ServerAddress>> segments, byte hashFunction) {
      super(topologyId, members);
      this.segments = segments;
      this.hashFunction = hashFunction;
   }

   @Override
   public String toString() {
      return "TestHashDistAware20Response{" +
             "hashFunction=" + hashFunction +
             ", topologyId=" + topologyId +
             ", members=" + members +
             '}';
   }
}
