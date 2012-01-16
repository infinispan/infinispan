package org.infinispan.configuration.cache;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.ch.ConsistentHash;

/**
 * Allows fine-tuning of rehashing characteristics. Must only used with 'distributed' cache mode.
 * 
 * @author pmuir
 * 
 */
public class HashConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder<HashConfiguration> {

   private ConsistentHash consistentHash;
   private Hash hash = new MurmurHash3();
   private int numOwners = 2;
   private int numVirtualNodes = 1;

   private final GroupsConfigurationBuilder groupsConfigurationBuilder;

   HashConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
      this.groupsConfigurationBuilder = new GroupsConfigurationBuilder(builder);
   }

   /**
    * The consistent hash in use.
    * 
    * NOTE: Currently Infinispan will not use the object instance, but instead instantiate a new
    * instance of the class. Therefore, do not expect any state to survive, and provide a no-args
    * constructor to any instance. This will be resolved in Infinispan 5.2.0
    */
   public HashConfigurationBuilder consistentHash(ConsistentHash consistentHash) {
      this.consistentHash = consistentHash;
      return this;
   }

   /**
    * Number of cluster-wide replicas for each cache entry.
    */
   public HashConfigurationBuilder numOwners(int numOwners) {
      this.numOwners = numOwners;
      return this;
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
    * 
    * @param numVirtualNodes the number of virtual nodes. Must be >0.
    * @throws IllegalArgumentException if numVirtualNodes <0
    */
   public HashConfigurationBuilder numVirtualNodes(int numVirtualNodes) {
      this.numVirtualNodes = numVirtualNodes;
      return this;
   }

   /**
    * Enable rebalancing and rehashing, which will take place when a new node joins the cluster or a
    * node leaves
    * @deprecated Use {@link StateTransferConfigurationBuilder#fetchInMemoryState(boolean)} instead.
    */
   public HashConfigurationBuilder rehashEnabled() {
      stateTransfer().fetchInMemoryState(true);
      return this;
   }
   
   /**
    * Enable rebalancing and rehashing, which will take place when a new node joins the cluster or a
    * node leaves
    * @deprecated Use {@link StateTransferConfigurationBuilder#fetchInMemoryState(boolean)} instead.
    */
   public HashConfigurationBuilder rehashEnabled(boolean enabled) {
      stateTransfer().fetchInMemoryState(enabled);
      return this;
   }

   /**
    * Disable rebalancing and rehashing, which would have taken place when a new node joins the
    * cluster or a node leaves
    * @deprecated Use {@link StateTransferConfigurationBuilder#fetchInMemoryState(boolean)} instead.
    */
   public HashConfigurationBuilder rehashDisabled() {
      stateTransfer().fetchInMemoryState(false);
      return this;
   }

   /**
    * Rehashing timeout
    * @deprecated Use {@link StateTransferConfigurationBuilder#timeout(long)} instead.
    */
   public HashConfigurationBuilder rehashRpcTimeout(long rehashRpcTimeout) {
      stateTransfer().timeout(rehashRpcTimeout);
      return this;
   }

   /**
    * @deprecated No longer used.
    */
   public HashConfigurationBuilder rehashWait(long rehashWait) {
      return this;
   }

   /**
    * The hash function in use. Used as a bit spreader and a general hash code generator. Typically
    * used in conjunction with the many default
    * {@link org.infinispan.distribution.ch.ConsistentHash} implementations shipped.
    * 
    * NOTE: Currently Infinispan will not use the object instance, but instead instantiate a new
    * instance of the class. Therefore, do not expect any state to survive, and provide a no-args
    * constructor to any instance. This will be resolved in Infinispan 5.2.0
    */
   public HashConfigurationBuilder hash(Hash hash) {
      this.hash = hash;
      return this;
   }

   public GroupsConfigurationBuilder groups() {
      return groupsConfigurationBuilder;
   }

   @Override
   void validate() {
      groupsConfigurationBuilder.validate();
   }

   @Override
   HashConfiguration create() {
      // TODO stateTransfer().create() will create a duplicate StateTransferConfiguration instance
      return new HashConfiguration(consistentHash, hash, numOwners, numVirtualNodes,
            groupsConfigurationBuilder.create(), stateTransfer().create());
   }

   @Override
   public HashConfigurationBuilder read(HashConfiguration template) {
      this.consistentHash = template.consistentHash();
      this.hash = template.hash();
      this.numOwners = template.numOwners();
      this.numVirtualNodes = template.numVirtualNodes();
      this.groupsConfigurationBuilder.read(template.groups());

      return this;
   }
   
}
