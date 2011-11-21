package org.infinispan.configuration.cache;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.HashSeed;
import org.infinispan.util.hash.Hash;

/**
 * Allows fine-tuning of rehashing characteristics. Must only used with 'distributed' cache mode.
 * 
 * @author pmuir
 * 
 */
public class HashConfiguration {

   private final ConsistentHash consistentHash;
   private final HashSeed hashSeed;
   private final Hash hash;
   private final int numOwners;
   private final int numVirtualNodes;
   private final boolean rehashEnabled;
   private final long rehashRpcTimeout;
   private final long rehashWait;
   private final GroupsConfiguration groupsConfiguration;

   HashConfiguration(ConsistentHash consistentHash, HashSeed hashSeed, Hash hash, int numOwners, int numVirtualNodes,
         boolean rehashEnabled, long rehashRpcTimeout, long rehashWait, GroupsConfiguration groupsConfiguration) {
      this.consistentHash = consistentHash;
      this.hashSeed = hashSeed;
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
    * A hash seed implementation which allows seed address to for consistent hash calculation to be
    * configured. This is particularly useful when Infinispan is accessed remotely and clients are
    * to calculate hash ids. Since clients are only aware of server endpoints, implementations of
    * {@link HashSeed} can seed based on this information instead of the traditional cluster
    * address.
    */
   public HashSeed hashSeed() {
      return hashSeed;
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
   public boolean isRehashEnabled() {
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
   public GroupsConfiguration groupsConfiguration() {
      return groupsConfiguration;
   }

}
