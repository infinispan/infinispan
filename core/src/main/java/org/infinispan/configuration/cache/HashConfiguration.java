package org.infinispan.configuration.cache;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHash;

/**
 * Allows fine-tuning of rehashing characteristics. Must only used with 'distributed' cache mode.
 * 
 * @author pmuir
 */
public class HashConfiguration {

   private final ConsistentHash consistentHash;
   private final Hash hash;
   private final int numOwners;
   private final int numVirtualNodes;
   private final GroupsConfiguration groupsConfiguration;
   private final StateTransferConfiguration stateTransferConfiguration;
   // For use by the LegacyConfigurationAdapter
   final boolean activated;

   HashConfiguration(ConsistentHash consistentHash, Hash hash, int numOwners, int numVirtualNodes,
                     GroupsConfiguration groupsConfiguration, StateTransferConfiguration stateTransferConfiguration, boolean activated) {
      this.consistentHash = consistentHash;
      this.hash = hash;
      this.numOwners = numOwners;
      this.numVirtualNodes = numVirtualNodes;
      this.groupsConfiguration = groupsConfiguration;
      this.stateTransferConfiguration = stateTransferConfiguration;
      this.activated = activated;
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
    * Controls the number of virtual nodes per "real" node. You can read more about virtual nodes in Infinispan's
    * <a href="https://docs.jboss.org/author/display/ISPN51">online user guide</a>.
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
    * @deprecated Use {@link org.infinispan.configuration.cache.StateTransferConfiguration#fetchInMemoryState()} instead.
    */
   public boolean rehashEnabled() {
      return stateTransferConfiguration.fetchInMemoryState();
   }

   /**
    * Rehashing timeout
    * @deprecated Use {@link org.infinispan.configuration.cache.StateTransferConfiguration#timeout()} instead.
    */
   public long rehashRpcTimeout() {
      return stateTransferConfiguration.timeout();
   }

   /**
    * @deprecated Use {@link org.infinispan.configuration.cache.StateTransferConfiguration#timeout()} instead.
    */
   public long rehashWait() {
      return stateTransferConfiguration.timeout();
   }
   
   /**
    * Configuration for various grouper definitions. See the user guide for more information.
    */
   public GroupsConfiguration groups() {
      return groupsConfiguration;
   }

}
