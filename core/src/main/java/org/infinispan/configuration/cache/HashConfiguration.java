package org.infinispan.configuration.cache;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHash;

/**
 * Allows fine-tuning of rehashing characteristics. Must only used with 'distributed' cache mode.
 * 
 * @author pmuir
 * 
 */
public class HashConfiguration {

   private final ConsistentHash consistentHash;
   private final Hash hash;
   private final int numOwners;
   private final int numVirtualNodes;
   private final boolean rehashEnabled;
   private final long rehashRpcTimeout;
   private final long rehashWait;
   private final GroupsConfiguration groupsConfiguration;

   HashConfiguration(ConsistentHash consistentHash, Hash hash, int numOwners, int numVirtualNodes,
         boolean rehashEnabled, long rehashRpcTimeout, long rehashWait, GroupsConfiguration groupsConfiguration) {
      this.consistentHash = consistentHash;
      this.hash = hash;
      this.numOwners = numOwners;
      this.numVirtualNodes = numVirtualNodes;
      this.rehashEnabled = rehashEnabled;
      this.rehashRpcTimeout = rehashRpcTimeout;
      this.rehashWait = rehashWait;
      this.groupsConfiguration = groupsConfiguration;
   }

   /**
    * The consistent hash in use.
    */
   public ConsistentHash consistentHash() {
      return consistentHash;
   }

   /**
    * The hash function in use. Used as a bit spreader and a general hash code generator.
    * Typically one of the the many default {@link org.infinispan.distribution.ch.ConsistentHash}
    * implementations shipped.
    */
   public Hash hash() {
      return hash;
   }

   /**
    * Number of cluster-wide replicas for each cache entry.
    */
   public int numOwners() {
      return numOwners;
   }

   /**
    * <p>
    * Controls the number of virtual nodes per "real" node. You can read more about virtual nodes at
    * TODO
    * </p>
    * 
    * <p>
    * If numVirtualNodes is 1, then virtual nodes are disabled. The topology aware consistent hash
    * must be used if you wish to take advnatage of virtual nodes.
    * </p>
    * 
    * <p>
    * A default of 1 is used.
    * </p>
    */
   public int numVirtualNodes() {
      return numVirtualNodes;
   }

   /**
    * If false, no rebalancing or rehashing will take place when a new node joins the cluster or a
    * node leaves
    */
   public boolean rehashEnabled() {
      return rehashEnabled;
   }

   /**
    * Rehashing timeout
    */
   public long rehashRpcTimeout() {
      return rehashRpcTimeout;
   }

   /**
    * 
    */
   public long rehashWait() {
      return rehashWait;
   }
   
   /**
    * Configuration for various grouper definitions. See the user guide for more information.
    */
   public GroupsConfiguration groups() {
      return groupsConfiguration;
   }

}
