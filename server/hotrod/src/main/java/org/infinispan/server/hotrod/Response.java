package org.infinispan.server.hotrod;

import java.util.Map;

import org.infinispan.remoting.transport.Address;

abstract class AbstractTopologyResponse {
   final int topologyId;
   final Map<Address, ServerAddress> serverEndpointsMap;
   final int numSegments;

   protected AbstractTopologyResponse(int topologyId, Map<Address, ServerAddress> serverEndpointsMap, int numSegments) {
      this.topologyId = topologyId;
      this.serverEndpointsMap = serverEndpointsMap;
      this.numSegments = numSegments;
   }
}

abstract class AbstractHashDistAwareResponse extends AbstractTopologyResponse {

   final int numOwners;
   final byte hashFunction;
   final int hashSpace;

   protected AbstractHashDistAwareResponse(int topologyId, Map<Address, ServerAddress> serverEndpointsMap,
                                           int numSegments, int numOwners, byte hashFunction, int hashSpace) {
      super(topologyId, serverEndpointsMap, numSegments);
      this.numOwners = numOwners;
      this.hashFunction = hashFunction;
      this.hashSpace = hashSpace;
   }
}

class TopologyAwareResponse extends AbstractTopologyResponse {

   protected TopologyAwareResponse(int topologyId, Map<Address, ServerAddress> serverEndpointsMap, int numSegments) {
      super(topologyId, serverEndpointsMap, numSegments);
   }

   @Override
   public String toString() {
      return "TopologyAwareResponse{" +
             "topologyId=" + topologyId +
             ", numSegments=" + numSegments +
             '}';
   }
}

class HashDistAwareResponse extends AbstractHashDistAwareResponse {

   protected HashDistAwareResponse(int topologyId, Map<Address, ServerAddress> serverEndpointsMap, int numSegments, int numOwners, byte hashFunction, int hashSpace) {
      super(topologyId, serverEndpointsMap, numSegments, numOwners, hashFunction, hashSpace);
   }

   @Override
   public String toString() {
      return "HashDistAwareResponse{" +
             "topologyId=" + topologyId +
             ", numSegments=" + numSegments +
             ", numOwners=" + numOwners +
             ", hashFunction=" + hashFunction +
             ", hashSpace=" + hashSpace +
             '}';
   }
}

class HashDistAware11Response extends AbstractHashDistAwareResponse {
   final int numVNodes;

   protected HashDistAware11Response(int topologyId, Map<Address, ServerAddress> serverEndpointsMap, int numOwners,
                                     byte hashFunction, int hashSpace, int numVNodes) {
      super(topologyId, serverEndpointsMap, 0, numOwners, hashFunction, hashSpace);
      this.numVNodes = numVNodes;
   }

   @Override
   public String toString() {
      return "HashDistAware11Response{" +
             "topologyId=" + topologyId +
             ", numSegments=" + numSegments +
             ", numOwners=" + numOwners +
             ", hashFunction=" + hashFunction +
             ", hashSpace=" + hashSpace +
             ", numVNodes=" + numVNodes +
             '}';
   }
}

class HashDistAware20Response extends AbstractTopologyResponse {
   final byte hashFunction;

   protected HashDistAware20Response(int topologyId, Map<Address, ServerAddress> serverEndpointsMap, int numSegments,
                                     byte hashFunction) {
      super(topologyId, serverEndpointsMap, numSegments);
      this.hashFunction = hashFunction;
   }

   @Override
   public String toString() {
      return "HashDistAware20Response{" +
             "topologyId=" + topologyId +
             ", numSegments=" + numSegments +
             ", hashFunction=" + hashFunction +
             '}';
   }
}
